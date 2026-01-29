"""
Google Ads Video Performance Sync
Fetches video performance stats from Google Ads and syncs to BigQuery.
Uses direct REST API calls (same approach as working automation).
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
logger.info("Google Ads Sync script initialized")


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
        raise Exception(f"[{response.status_code}] Failed to get access token. {response.text}")
    
    return response.json()['access_token']


class GoogleAdsVideoSync:
    def __init__(self):
        # BigQuery Config
        self.creds_json = os.getenv('GOOGLE_CREDENTIALS')
        if not self.creds_json:
            raise ValueError("GOOGLE_CREDENTIALS environment variable is required")
        
        self.creds_dict = json.loads(self.creds_json)
        self.bq_client = bigquery.Client.from_service_account_info(self.creds_dict)
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'ads_data_staging')
        self.table_id = os.getenv('BIGQUERY_TABLE', 'clean_ads_transparency')
        self.full_table_id = f"{self.bq_client.project}.{self.dataset_id}.{self.table_id}"

        # Google Ads Config - SAME as your working automation
        self.client_id = os.getenv('GOOGLE_ADS_CLIENT_ID')
        self.client_secret = os.getenv('GOOGLE_ADS_CLIENT_SECRET')
        self.refresh_token = os.getenv('GOOGLE_ADS_REFRESH_TOKEN')
        self.dev_token = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
        
        if not all([self.client_id, self.client_secret, self.refresh_token, self.dev_token]):
            raise ValueError("Missing Google Ads credentials")
        
        # Use v20 - same as your working automation
        self.google_ads_api_url = 'https://googleads.googleapis.com/v20/customers/'
        
        # Get access token
        logger.info("Getting access token...")
        self.access_token = get_access_token(self.client_id, self.client_secret, self.refresh_token)
        logger.info(f"Access token obtained (length: {len(self.access_token)})")
        logger.info(f"Developer token: {self.dev_token[:10]}...")

    def get_request_headers(self, login_customer_id=None):
        """Get request headers for API calls - same as your working code"""
        headers = {
            'developer-token': self.dev_token,
            'Authorization': f'Bearer {self.access_token}'
        }
        
        if login_customer_id:
            headers['login-customer-id'] = login_customer_id
        
        return headers

    def search(self, query: str, customer_id: str):
        """Execute a search query - same pattern as your working ads_service.py"""
        target_customer_id = customer_id.replace('-', '')
        
        url = f"{self.google_ads_api_url}{target_customer_id}/googleAds:search"
        request_data = {'query': query}
        
        logger.info(f"    Calling: {url}")
        
        response = requests.post(
            url,
            json=request_data,
            headers=self.get_request_headers(login_customer_id=target_customer_id)
        )
        
        logger.info(f"    Response Status: {response.status_code}")
        
        if response.status_code != 200:
            try:
                error_text = response.text[:1500]
            except:
                error_text = "Unable to get error text"
            logger.warning(f"    API Error: {error_text}")
            return {'result': False, 'response': [], 'errorMsg': error_text}
        
        try:
            result = response.json()
        except Exception as e:
            logger.warning(f"    Failed to parse JSON: {e}")
            return {'result': False, 'response': [], 'errorMsg': str(e)}
        
        if 'results' in result:
            return {'result': True, 'response': result['results']}
        
        # Empty result set is OK
        return {'result': True, 'response': []}

    def hex_to_youtube_id(self, hex_id):
        """Standard conversion used in clean_data.py"""
        if not hex_id or len(hex_id) < 10:
            return None
        try:
            video_bytes = bytes.fromhex(hex_id.strip().lower())
            b64 = base64.b64encode(video_bytes).decode('utf-8')
            return b64.replace('+', '-').replace('/', '_').rstrip('=')
        except:
            return None

    def get_video_stats(self, customer_id):
        """Query Google Ads for video stats from multiple sources"""
        stats = []
        seen_keys = set()
        
        # Query 1: Campaign-level assets
        query1 = """
            SELECT
              asset.youtube_video_asset.youtube_video_id,
              campaign.app_campaign_setting.app_id,
              campaign.id,
              campaign.name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              customer.id,
              customer.descriptive_name
            FROM campaign_asset
            WHERE asset.type = 'YOUTUBE_VIDEO'
        """
        
        logger.info(f"    Query 1: campaign_asset (all videos)...")
        result1 = self.search(query1, customer_id)
        
        if result1['result'] and result1['response']:
            logger.info(f"      Raw rows returned: {len(result1['response'])}")
            for row in result1['response']:
                self._process_video_row(row, stats, seen_keys, 'campaign_asset')
        else:
            logger.info(f"      No data from campaign_asset")
        
        # Query 2: Ad Group-level assets  
        query2 = """
            SELECT
              asset.youtube_video_asset.youtube_video_id,
              campaign.app_campaign_setting.app_id,
              campaign.id,
              campaign.name,
              ad_group.id,
              ad_group.name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              customer.id,
              customer.descriptive_name
            FROM ad_group_asset
            WHERE asset.type = 'YOUTUBE_VIDEO'
        """
        
        logger.info(f"    Query 2: ad_group_asset (all videos)...")
        result2 = self.search(query2, customer_id)
        
        if result2['result'] and result2['response']:
            logger.info(f"      Raw rows returned: {len(result2['response'])}")
            for row in result2['response']:
                self._process_video_row(row, stats, seen_keys, 'ad_group_asset')
        else:
            logger.info(f"      No data from ad_group_asset")
        
        # Query 3: Check what asset types exist in the account
        query3 = """
            SELECT
              asset.type,
              asset.resource_name
            FROM asset
            WHERE asset.type IN ('YOUTUBE_VIDEO', 'MEDIA_BUNDLE', 'IMAGE')
            LIMIT 20
        """
        
        logger.info(f"    Query 3: Checking what assets exist...")
        result3 = self.search(query3, customer_id)
        
        if result3['result'] and result3['response']:
            asset_types = {}
            for row in result3['response']:
                asset_type = row.get('asset', {}).get('type', 'UNKNOWN')
                asset_types[asset_type] = asset_types.get(asset_type, 0) + 1
            logger.info(f"      Asset types found: {asset_types}")
        else:
            logger.info(f"      No assets found in account")
        
        logger.info(f"    Total video stats for this customer: {len(stats)}")
        return stats

    def _process_video_row(self, row, stats, seen_keys, source):
        """Process a video row from Google Ads API response"""
        try:
            asset = row.get('asset', {})
            campaign = row.get('campaign', {})
            metrics = row.get('metrics', {})
            customer = row.get('customer', {})
            ad_group = row.get('adGroup', {})
            
            youtube_id = asset.get('youtubeVideoAsset', {}).get('youtubeVideoId')
            if not youtube_id:
                return
            
            # Create dedup key
            key = (
                str(customer.get('id', '')),
                str(campaign.get('id', '')),
                str(ad_group.get('id', '')),
                youtube_id
            )
            
            if key in seen_keys:
                return
            seen_keys.add(key)
            
            stats.append({
                'youtube_id': youtube_id,
                'package_id': campaign.get('appCampaignSetting', {}).get('appId', 'N/A'),
                'campaign_id': str(campaign.get('id', '')),
                'campaign_name': campaign.get('name', ''),
                'impressions': int(metrics.get('impressions', 0)),
                'clicks': int(metrics.get('clicks', 0)),
                'cost': float(metrics.get('costMicros', 0)) / 1000000.0,
                'account_id': str(customer.get('id', '')),
                'account_name': customer.get('descriptiveName', ''),
                'ad_group_id': str(ad_group.get('id', 'N/A')),
                'ad_group_name': ad_group.get('name', 'N/A'),
                'source': source
            })
            
            logger.info(f"      Found video: {youtube_id} ({source})")
            
        except Exception as e:
            logger.debug(f"Error parsing row: {e}")


    def init_bigquery(self):
        """Ensure dataset exists"""
        dataset_ref = bigquery.DatasetReference(self.bq_client.project, self.dataset_id)
        try:
            self.bq_client.get_dataset(dataset_ref)
        except:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.bq_client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")

    def run(self):
        logger.info("Starting Google Ads Video Sync...")
        
        # Initialize BigQuery
        self.init_bigquery()
        
        # 1. Load video map from BigQuery
        query = f"SELECT video_id, youtube_url FROM `{self.full_table_id}`"
        df_videos = self.bq_client.query(query).to_dataframe()
        logger.info(f"Loaded {len(df_videos)} videos from BigQuery.")
        
        # Create mapping: hex_id -> youtube_id
        hex_to_yt = {}
        yt_to_hex = {}
        for _, row in df_videos.iterrows():
            hex_id = row['video_id']
            yt_id = self.hex_to_youtube_id(hex_id)
            if yt_id:
                hex_to_yt[hex_id] = yt_id
                yt_to_hex[yt_id] = hex_id
        
        logger.info(f"Mapped {len(yt_to_hex)} videos for matching.")
        
        # 2. Get customer IDs from environment
        customer_ids_env = os.getenv('GOOGLE_ADS_CUSTOMER_IDS', '')
        if not customer_ids_env:
            logger.error("GOOGLE_ADS_CUSTOMER_IDS environment variable is required")
            return
        
        customer_ids = [cid.strip().replace("-", "") for cid in customer_ids_env.split(',') if cid.strip()]
        logger.info(f"Processing {len(customer_ids)} customer accounts...")
        
        # 3. Fetch video stats from all accounts
        all_stats = []
        for cid in customer_ids:
            logger.info(f"Processing Customer: {cid}")
            stats = self.get_video_stats(cid)
            all_stats.extend(stats)
        
        if not all_stats:
            logger.info("No video stats found in Google Ads.")
            return
        
        logger.info(f"Total raw stats collected: {len(all_stats)}")
        
        # 4. Aggregate stats by (youtube_id, package_id)
        df_ads = pd.DataFrame(all_stats)
        
        # Deduplicate
        df_ads = df_ads.drop_duplicates(subset=['account_id', 'campaign_id', 'youtube_id', 'package_id'])
        
        # Filter: only keep stats for videos we have in our database
        df_ads['video_id'] = df_ads['youtube_id'].map(yt_to_hex)
        df_ads = df_ads.dropna(subset=['video_id'])
        
        if df_ads.empty:
            logger.info("None of the videos in Google Ads match the BigQuery database.")
            return
        
        logger.info(f"Matched {len(df_ads)} video stats with BigQuery data.")
        
        # Group and sum metrics
        agg_functions = {
            'impressions': 'sum',
            'clicks': 'sum',
            'cost': 'sum',
            'account_name': lambda x: ', '.join(sorted(set(str(v) for v in x if v)))
        }
        df_agg = df_ads.groupby(['video_id', 'package_id']).agg(agg_functions).reset_index()

        # 5. Upload to BigQuery
        stats_table_id = f"{self.bq_client.project}.{self.dataset_id}.video_performance_stats"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        
        logger.info(f"Uploading {len(df_agg)} aggregated stats rows to {stats_table_id}...")
        job = self.bq_client.load_table_from_dataframe(df_agg, stats_table_id, job_config=job_config)
        job.result()
        
        # 6. Create Comparison View
        self.create_comparison_view()
        
        logger.info("✓ Google Ads Sync Complete!")

    def create_comparison_view(self):
        view_id = f"{self.bq_client.project}.{self.dataset_id}.video_stats_comparison"
        view_query = f"""
            SELECT 
              t.video_id,
              t.youtube_url,
              t.app_name,
              t.advertiser_name,
              s.package_id,
              t.views as organic_youtube_views,
              t.upload_time,
              s.impressions as google_ads_impressions,
              s.clicks as google_ads_clicks,
              s.cost as google_ads_cost,
              s.account_name as google_ads_accounts,
              t.last_updated as last_sync_organic,
              CURRENT_TIMESTAMP() as last_sync_ads
            FROM `{self.full_table_id}` t
            LEFT JOIN `{self.bq_client.project}.{self.dataset_id}.video_performance_stats` s 
              ON t.video_id = s.video_id
        """
        
        view = bigquery.Table(view_id)
        view.view_query = view_query
        
        try:
            self.bq_client.delete_table(view_id, not_found_ok=True)
            self.bq_client.create_table(view)
            logger.info(f"Created comparison view: {view_id}")
        except Exception as e:
            logger.warning(f"Could not create view: {e}")


if __name__ == "__main__":
    try:
        sync = GoogleAdsVideoSync()
        sync.run()
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise
