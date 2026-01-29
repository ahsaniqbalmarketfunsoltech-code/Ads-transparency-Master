"""
Google Ads Video Performance Sync
Fetches video performance stats from Google Ads and updates clean_ads_transparency table.
Uses batch MERGE for fast updates.
"""

import os
import json
import logging
import base64
import requests
from datetime import datetime
from pathlib import Path
import pandas as pd
from google.cloud import bigquery

# Setup logging
Path("logs").mkdir(exist_ok=True)
log_filename = f"logs/google_ads_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_filename)
    ]
)
logger = logging.getLogger(__name__)


def get_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    """Gets access token from refresh token."""
    response = requests.post(
        'https://oauth2.googleapis.com/token',
        data={
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        }
    )
    if response.status_code < 200 or response.status_code >= 300:
        raise Exception(f"Failed to get access token: {response.text}")
    return response.json()['access_token']


class GoogleAdsVideoSync:
    def __init__(self):
        # BigQuery Config
        self.creds_json = os.getenv('GOOGLE_CREDENTIALS')
        if not self.creds_json:
            raise ValueError("GOOGLE_CREDENTIALS required")
        
        self.creds_dict = json.loads(self.creds_json)
        self.bq_client = bigquery.Client.from_service_account_info(self.creds_dict)
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'ads_data_staging')
        self.table_id = os.getenv('BIGQUERY_TABLE', 'clean_ads_transparency')
        self.full_table_id = f"{self.bq_client.project}.{self.dataset_id}.{self.table_id}"

        # Google Ads Config
        self.client_id = os.getenv('GOOGLE_ADS_CLIENT_ID')
        self.client_secret = os.getenv('GOOGLE_ADS_CLIENT_SECRET')
        self.refresh_token = os.getenv('GOOGLE_ADS_REFRESH_TOKEN')
        self.dev_token = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
        
        if not all([self.client_id, self.client_secret, self.refresh_token, self.dev_token]):
            raise ValueError("Missing Google Ads credentials")
        
        self.google_ads_api_url = 'https://googleads.googleapis.com/v20/customers/'
        self.access_token = get_access_token(self.client_id, self.client_secret, self.refresh_token)

    def get_request_headers(self, login_customer_id=None):
        headers = {
            'developer-token': self.dev_token,
            'Authorization': f'Bearer {self.access_token}'
        }
        if login_customer_id:
            headers['login-customer-id'] = login_customer_id
        return headers

    def search(self, query: str, customer_id: str):
        """Execute a search query"""
        target_customer_id = customer_id.replace('-', '')
        url = f"{self.google_ads_api_url}{target_customer_id}/googleAds:search"
        
        response = requests.post(
            url,
            json={'query': query},
            headers=self.get_request_headers(login_customer_id=target_customer_id)
        )
        
        if response.status_code != 200:
            return {'result': False, 'response': []}
        
        try:
            result = response.json()
            return {'result': True, 'response': result.get('results', [])}
        except:
            return {'result': False, 'response': []}

    def get_asset_info_map(self, customer_id):
        """Get mapping of asset resource names to YouTube video IDs and status"""
        query = """
            SELECT
              asset.resource_name,
              asset.youtube_video_asset.youtube_video_id,
              asset.policy_summary.approval_status
            FROM asset
            WHERE asset.type = 'YOUTUBE_VIDEO'
        """
        result = self.search(query, customer_id)
        
        asset_map = {}
        if result['result'] and result['response']:
            for row in result['response']:
                asset = row.get('asset', {})
                resource_name = asset.get('resourceName', '')
                youtube_id = asset.get('youtubeVideoAsset', {}).get('youtubeVideoId', '')
                approval_status = asset.get('policySummary', {}).get('approvalStatus', 'UNKNOWN')
                
                if resource_name and youtube_id:
                    asset_map[resource_name] = {
                        'youtube_id': youtube_id,
                        'approval_status': approval_status
                    }
        return asset_map

    def get_video_stats(self, customer_id):
        """Query Google Ads for video stats with status"""
        stats = []
        seen_keys = set()
        
        asset_map = self.get_asset_info_map(customer_id)
        
        # Query APP_AD
        query = """
            SELECT
              ad_group_ad.ad.app_ad.youtube_videos,
              ad_group_ad.status,
              ad_group.status,
              campaign.status,
              campaign.app_campaign_setting.app_id,
              customer.id,
              customer.descriptive_name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM ad_group_ad
            WHERE ad_group_ad.ad.type = 'APP_AD'
        """
        
        result = self.search(query, customer_id)
        if result['result'] and result['response']:
            for row in result['response']:
                self._process_row(row, stats, seen_keys, asset_map, 'appAd', 'youtubeVideos')
        
        # Query APP_ENGAGEMENT_AD
        query2 = """
            SELECT
              ad_group_ad.ad.app_engagement_ad.videos,
              ad_group_ad.status,
              ad_group.status,
              campaign.status,
              campaign.app_campaign_setting.app_id,
              customer.id,
              customer.descriptive_name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM ad_group_ad
            WHERE ad_group_ad.ad.type = 'APP_ENGAGEMENT_AD'
        """
        
        result2 = self.search(query2, customer_id)
        if result2['result'] and result2['response']:
            for row in result2['response']:
                self._process_row(row, stats, seen_keys, asset_map, 'appEngagementAd', 'videos')
        
        return stats

    def _process_row(self, row, stats, seen_keys, asset_map, ad_key, video_key):
        """Process row to extract video stats"""
        try:
            ad_group_ad = row.get('adGroupAd', {})
            ad = ad_group_ad.get('ad', {})
            campaign = row.get('campaign', {})
            metrics = row.get('metrics', {})
            customer = row.get('customer', {})
            ad_group = row.get('adGroup', {})
            
            # Get statuses
            ad_status = ad_group_ad.get('status', 'UNKNOWN')
            ag_status = ad_group.get('status', 'UNKNOWN')
            c_status = campaign.get('status', 'UNKNOWN')
            
            # Determine effective status
            if 'REMOVED' in [ad_status, ag_status, c_status]:
                effective_status = 'REMOVED'
            elif ad_status == 'ENABLED' and ag_status == 'ENABLED' and c_status == 'ENABLED':
                effective_status = 'RUNNING'
            else:
                effective_status = 'PAUSED'
            
            videos = ad.get(ad_key, {}).get(video_key, [])
            if not videos:
                return
            
            for video in videos:
                asset_resource = video.get('asset', '')
                asset_info = asset_map.get(asset_resource, {})
                youtube_id = asset_info.get('youtube_id')
                
                if not youtube_id:
                    continue
                
                youtube_url = f"https://www.youtube.com/watch?v={youtube_id}"
                
                if youtube_url in seen_keys:
                    # Aggregate
                    for stat in stats:
                        if stat['youtube_url'] == youtube_url:
                            stat['impressions'] += int(metrics.get('impressions', 0))
                            stat['clicks'] += int(metrics.get('clicks', 0))
                            stat['cost'] += float(metrics.get('costMicros', 0)) / 1000000.0
                            if effective_status == 'RUNNING':
                                stat['status'] = 'RUNNING'
                            elif effective_status == 'PAUSED' and stat['status'] != 'RUNNING':
                                stat['status'] = 'PAUSED'
                            break
                    continue
                
                seen_keys.add(youtube_url)
                
                stats.append({
                    'youtube_url': youtube_url,
                    'impressions': int(metrics.get('impressions', 0)),
                    'clicks': int(metrics.get('clicks', 0)),
                    'cost': float(metrics.get('costMicros', 0)) / 1000000.0,
                    'account_name': customer.get('descriptiveName', ''),
                    'status': effective_status
                })
        except:
            pass

    def ensure_columns_exist(self):
        """Add Google Ads columns if they don't exist"""
        columns = [
            ("google_ads_impressions", "INTEGER"),
            ("google_ads_clicks", "INTEGER"),
            ("google_ads_cost", "FLOAT64"),
            ("google_ads_accounts", "STRING"),
            ("google_ads_status", "STRING"),
            ("google_ads_last_sync", "TIMESTAMP")
        ]
        
        for col_name, col_type in columns:
            try:
                q = f"ALTER TABLE `{self.full_table_id}` ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                self.bq_client.query(q).result()
            except:
                pass
        
        logger.info("✓ Ensured Google Ads columns exist")

    def run(self):
        logger.info("=" * 50)
        logger.info("Google Ads Video Sync Started")
        logger.info("=" * 50)
        
        self.ensure_columns_exist()
        
        # Load youtube_urls from BigQuery
        query = f"SELECT youtube_url FROM `{self.full_table_id}` WHERE youtube_url IS NOT NULL"
        df_videos = self.bq_client.query(query).to_dataframe()
        valid_urls = set(df_videos['youtube_url'].dropna().tolist())
        logger.info(f"📊 Loaded {len(valid_urls)} videos from BigQuery")
        
        # Get customer IDs
        customer_ids_env = os.getenv('GOOGLE_ADS_CUSTOMER_IDS', '')
        if not customer_ids_env:
            logger.error("❌ GOOGLE_ADS_CUSTOMER_IDS required")
            return
        
        customer_ids = [cid.strip().replace("-", "") for cid in customer_ids_env.split(',') if cid.strip()]
        logger.info(f"🔍 Processing {len(customer_ids)} Google Ads accounts...")
        
        # Fetch video stats
        all_stats = []
        for i, cid in enumerate(customer_ids, 1):
            stats = self.get_video_stats(cid)
            all_stats.extend(stats)
            logger.info(f"   Account {i}/{len(customer_ids)}: {cid} → {len(stats)} videos")
        
        if not all_stats:
            logger.info("⚠️ No video stats found")
            return
        
        logger.info(f"📈 Total raw stats: {len(all_stats)}")
        
        # Convert to DataFrame and filter
        df_ads = pd.DataFrame(all_stats)
        df_ads = df_ads[df_ads['youtube_url'].isin(valid_urls)]
        
        if df_ads.empty:
            logger.info("⚠️ No matching videos found")
            return
        
        logger.info(f"✓ Matched {len(df_ads)} stats with BigQuery")
        
        # Aggregate by youtube_url
        def agg_status(x):
            if 'RUNNING' in x.values:
                return 'RUNNING'
            elif 'PAUSED' in x.values:
                return 'PAUSED'
            return 'REMOVED'
        
        df_agg = df_ads.groupby('youtube_url').agg({
            'impressions': 'sum',
            'clicks': 'sum',
            'cost': 'sum',
            'account_name': lambda x: ', '.join(sorted(set(str(v) for v in x if v))),
            'status': agg_status
        }).reset_index()
        
        logger.info(f"📊 Aggregated to {len(df_agg)} unique videos")
        
        status_counts = df_agg['status'].value_counts().to_dict()
        logger.info(f"   Status: {status_counts}")
        
        # Upload to temp table then MERGE (much faster than individual updates)
        logger.info("💾 Uploading to temp table...")
        
        temp_table_id = f"{self.bq_client.project}.{self.dataset_id}.temp_google_ads_stats"
        
        # Prepare DataFrame for upload
        df_upload = df_agg.rename(columns={
            'impressions': 'google_ads_impressions',
            'clicks': 'google_ads_clicks',
            'cost': 'google_ads_cost',
            'account_name': 'google_ads_accounts',
            'status': 'google_ads_status'
        })
        
        # Upload to temp table
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = self.bq_client.load_table_from_dataframe(df_upload, temp_table_id, job_config=job_config)
        job.result()
        
        logger.info("💾 Merging into main table...")
        
        # MERGE statement - single fast operation
        merge_query = f"""
            MERGE `{self.full_table_id}` T
            USING `{temp_table_id}` S
            ON T.youtube_url = S.youtube_url
            WHEN MATCHED THEN
                UPDATE SET
                    T.google_ads_impressions = S.google_ads_impressions,
                    T.google_ads_clicks = S.google_ads_clicks,
                    T.google_ads_cost = S.google_ads_cost,
                    T.google_ads_accounts = S.google_ads_accounts,
                    T.google_ads_status = S.google_ads_status,
                    T.google_ads_last_sync = CURRENT_TIMESTAMP()
        """
        
        self.bq_client.query(merge_query).result()
        
        # Clean up temp table
        self.bq_client.delete_table(temp_table_id, not_found_ok=True)
        
        logger.info(f"✓ Updated {len(df_agg)} videos in clean_ads_transparency")
        logger.info("=" * 50)
        logger.info("✅ Google Ads Sync Complete!")
        logger.info("=" * 50)


if __name__ == "__main__":
    try:
        sync = GoogleAdsVideoSync()
        sync.run()
    except Exception as e:
        logger.error(f"❌ Sync failed: {e}")
        raise
