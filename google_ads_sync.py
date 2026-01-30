"""
Google Ads Video Performance Sync - FULL DETAILS VERSION
Fetches ALL available video stats from Google Ads and updates clean_ads_transparency table.
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
              asset.youtube_video_asset.youtube_video_title,
              asset.policy_summary.approval_status,
              asset.policy_summary.review_status
            FROM asset
            WHERE asset.type = 'YOUTUBE_VIDEO'
        """
        result = self.search(query, customer_id)
        
        asset_map = {}
        if result['result'] and result['response']:
            for row in result['response']:
                asset = row.get('asset', {})
                resource_name = asset.get('resourceName', '')
                yt_asset = asset.get('youtubeVideoAsset', {})
                youtube_id = yt_asset.get('youtubeVideoId', '')
                youtube_title = yt_asset.get('youtubeVideoTitle', '')
                policy = asset.get('policySummary', {})
                approval_status = policy.get('approvalStatus', 'N/A')
                review_status = policy.get('reviewStatus', 'N/A')
                
                if resource_name and youtube_id:
                    asset_map[resource_name] = {
                        'youtube_id': youtube_id,
                        'youtube_title': youtube_title,
                        'approval_status': approval_status if approval_status else 'N/A',
                        'review_status': review_status if review_status else 'N/A'
                    }
        return asset_map

    def get_video_stats(self, customer_id):
        """Query Google Ads for ALL video stats and details"""
        stats = []
        seen_keys = set()
        
        asset_map = self.get_asset_info_map(customer_id)
        
        # Query APP_AD with ALL available metrics
        query = """
            SELECT
              ad_group_ad.ad.app_ad.youtube_videos,
              ad_group_ad.ad.id,
              ad_group_ad.status,
              ad_group.id,
              ad_group.name,
              ad_group.status,
              campaign.id,
              campaign.name,
              campaign.status,
              campaign.app_campaign_setting.app_id,
              campaign.app_campaign_setting.app_store,
              campaign.advertising_channel_type,
              campaign.advertising_channel_sub_type,
              campaign.start_date,
              campaign.end_date,
              customer.id,
              customer.descriptive_name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.conversions_value,
              metrics.video_views,
              metrics.video_quartile_p25_rate,
              metrics.video_quartile_p50_rate,
              metrics.video_quartile_p75_rate,
              metrics.video_quartile_p100_rate,
              metrics.average_cpc,
              metrics.average_cpm,
              metrics.ctr,
              metrics.all_conversions,
              metrics.interactions
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
              ad_group_ad.ad.id,
              ad_group_ad.status,
              ad_group.id,
              ad_group.name,
              ad_group.status,
              campaign.id,
              campaign.name,
              campaign.status,
              campaign.app_campaign_setting.app_id,
              campaign.app_campaign_setting.app_store,
              campaign.advertising_channel_type,
              campaign.advertising_channel_sub_type,
              campaign.start_date,
              campaign.end_date,
              customer.id,
              customer.descriptive_name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.conversions_value,
              metrics.video_views,
              metrics.video_quartile_p25_rate,
              metrics.video_quartile_p50_rate,
              metrics.video_quartile_p75_rate,
              metrics.video_quartile_p100_rate,
              metrics.average_cpc,
              metrics.average_cpm,
              metrics.ctr,
              metrics.all_conversions,
              metrics.interactions
            FROM ad_group_ad
            WHERE ad_group_ad.ad.type = 'APP_ENGAGEMENT_AD'
        """
        
        result2 = self.search(query2, customer_id)
        if result2['result'] and result2['response']:
            for row in result2['response']:
                self._process_row(row, stats, seen_keys, asset_map, 'appEngagementAd', 'videos')
        
        return stats

    def _process_row(self, row, stats, seen_keys, asset_map, ad_key, video_key):
        """Process row to extract ALL video stats"""
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
            
            # Campaign details
            app_setting = campaign.get('appCampaignSetting', {})
            
            for video in videos:
                asset_resource = video.get('asset', '')
                asset_info = asset_map.get(asset_resource, {})
                youtube_id = asset_info.get('youtube_id')
                
                if not youtube_id:
                    continue
                
                youtube_url = f"https://www.youtube.com/watch?v={youtube_id}"
                
                if youtube_url in seen_keys:
                    # Aggregate metrics for same video
                    for stat in stats:
                        if stat['youtube_url'] == youtube_url:
                            stat['impressions'] += int(metrics.get('impressions', 0))
                            stat['clicks'] += int(metrics.get('clicks', 0))
                            stat['cost'] += float(metrics.get('costMicros', 0)) / 1000000.0
                            stat['conversions'] += float(metrics.get('conversions', 0))
                            stat['conversions_value'] += float(metrics.get('conversionsValue', 0))
                            stat['video_views'] += int(metrics.get('videoViews', 0))
                            stat['interactions'] += int(metrics.get('interactions', 0))
                            stat['all_conversions'] += float(metrics.get('allConversions', 0))
                            if effective_status == 'RUNNING':
                                stat['status'] = 'RUNNING'
                            elif effective_status == 'PAUSED' and stat['status'] != 'RUNNING':
                                stat['status'] = 'PAUSED'
                            break
                    continue
                
                seen_keys.add(youtube_url)
                
                # Status Handling
                stats.append({
                    'youtube_url': youtube_url,
                    'youtube_id': youtube_id,
                    'video_title': asset_info.get('youtube_title', ''),
                    
                    # Performance Metrics
                    'impressions': int(metrics.get('impressions', 0)),
                    'clicks': int(metrics.get('clicks', 0)),
                    'cost': float(metrics.get('costMicros', 0)) / 1000000.0,
                    'conversions': float(metrics.get('conversions', 0)),
                    'conversions_value': float(metrics.get('conversionsValue', 0)),
                    'video_views': int(metrics.get('videoViews', 0)),
                    'interactions': int(metrics.get('interactions', 0)),
                    'all_conversions': float(metrics.get('allConversions', 0)),
                    
                    # Rates (these are averages, not summed)
                    'ctr': float(metrics.get('ctr', 0)),
                    'avg_cpc': float(metrics.get('averageCpc', 0)) / 1000000.0,
                    'avg_cpm': float(metrics.get('averageCpm', 0)) / 1000000.0,
                    'video_25_rate': float(metrics.get('videoQuartileP25Rate', 0)),
                    'video_50_rate': float(metrics.get('videoQuartileP50Rate', 0)),
                    'video_75_rate': float(metrics.get('videoQuartileP75Rate', 0)),
                    'video_100_rate': float(metrics.get('videoQuartileP100Rate', 0)),
                    
                    # Campaign Info
                    'campaign_name': campaign.get('name', ''),
                    'campaign_id': str(campaign.get('id', '')),
                    'campaign_status': c_status,
                    'campaign_type': campaign.get('advertisingChannelType', ''),
                    'campaign_subtype': campaign.get('advertisingChannelSubType', ''),
                    'campaign_start_date': campaign.get('startDate', ''),
                    'campaign_end_date': campaign.get('endDate', ''),
                    
                    # App Info
                    'package_id': app_setting.get('appId', ''),
                    'app_store': app_setting.get('appStore', ''),
                    
                    # Ad Group Info
                    'ad_group_name': ad_group.get('name', ''),
                    'ad_group_id': str(ad_group.get('id', '')),
                    'ad_group_status': ag_status,
                    
                    # Ad Info
                    'ad_id': str(ad.get('id', '')),
                    'ad_status': ad_status,
                    
                    # Account Info
                    'account_name': customer.get('descriptiveName', ''),
                    'account_id': str(customer.get('id', '')),
                    
                    # Status with user-friendly labels
                    'status': effective_status,
                    'approval_status': asset_info.get('approval_status', 'N/A').replace('_', ' '),
                    'review_status': 'PENDING' if asset_info.get('review_status') == 'UNDER_REVIEW' else asset_info.get('review_status', 'N/A')
                })
        except:
            pass

    def ensure_columns_exist(self):
        """Add ALL Google Ads columns. Only drop and recreate if type is not STRING."""
        
        # Get existing column types to avoid unnecessary drops
        table = self.bq_client.get_table(self.full_table_id)
        existing_cols = {field.name: field.field_type for field in table.schema}
        
        # Performance Metrics that need to be STRING for formatted display
        metrics_to_format = [
            "google_ads_impressions", "google_ads_clicks", "google_ads_cost",
            "google_ads_conversions", "google_ads_conversions_value",
            "google_ads_video_views", "google_ads_interactions",
            "google_ads_all_conversions", "google_ads_ctr", "google_ads_avg_cpc",
            "google_ads_avg_cpm", "google_ads_video_25_rate",
            "google_ads_video_50_rate", "google_ads_video_75_rate",
            "google_ads_video_100_rate"
        ]
        
        # 1. Drop only if the type is NOT string (one-time migration)
        for col in metrics_to_format:
            if col in existing_cols and existing_cols[col] != 'STRING':
                logger.info(f"Converting column {col} to STRING...")
                try:
                    self.bq_client.query(f"ALTER TABLE `{self.full_table_id}` DROP COLUMN {col}").result()
                    del existing_cols[col] # Mark as removed
                except Exception as e:
                    logger.error(f"Failed to drop {col}: {e}")

        # 2. Define all columns with their correct metadata
        columns = [
            # Counts (Comma separated)
            ("google_ads_impressions", "STRING"),
            ("google_ads_clicks", "STRING"),
            ("google_ads_video_views", "STRING"),
            ("google_ads_interactions", "STRING"),
            
            # Monetary (With $ sign)
            ("google_ads_cost", "STRING"),
            ("google_ads_conversions_value", "STRING"),
            
            # Decimals (No $ sign, plain decimal like 0.02)
            ("google_ads_avg_cpc", "STRING"),
            ("google_ads_avg_cpm", "STRING"),
            ("google_ads_conversions", "STRING"),
            ("google_ads_all_conversions", "STRING"),
            
            # Percentages (With % sign)
            ("google_ads_ctr", "STRING"),
            ("google_ads_video_25_rate", "STRING"),
            ("google_ads_video_50_rate", "STRING"),
            ("google_ads_video_75_rate", "STRING"),
            ("google_ads_video_100_rate", "STRING"),
            
            # Campaign & Metadata
            ("google_ads_campaign_name", "STRING"),
            ("google_ads_campaign_id", "STRING"),
            ("google_ads_campaign_status", "STRING"),
            ("google_ads_campaign_type", "STRING"),
            ("google_ads_campaign_subtype", "STRING"),
            ("google_ads_package_id", "STRING"),
            ("google_ads_app_store", "STRING"),
            ("google_ads_ad_group_name", "STRING"),
            ("google_ads_ad_group_id", "STRING"),
            ("google_ads_account_name", "STRING"),
            ("google_ads_account_id", "STRING"),
            ("google_ads_status", "STRING"),
            ("google_ads_approval_status", "STRING"),
            ("google_ads_review_status", "STRING"),
            ("google_ads_last_sync", "TIMESTAMP")
        ]
        
        for col_name, col_type in columns:
            if col_name not in existing_cols:
                try:
                    q = f"ALTER TABLE `{self.full_table_id}` ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                    self.bq_client.query(q).result()
                    logger.info(f"Added column: {col_name}")
                except Exception as e:
                    logger.debug(f"Could not add {col_name}: {e}")
        
        logger.info("✓ Smart column check complete. All metrics have separate STRING columns.")

    def run(self):
        logger.info("=" * 50)
        logger.info("Google Ads Video Sync - FULL DETAILS")
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
        logger.info(f"🔍 Processing {len(customer_ids)} accounts...")
        
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
        
        logger.info(f"✓ Matched {len(df_ads)} videos")
        
        # For aggregation, sum metrics but keep first value for non-summable fields
        agg_dict = {
            # Sum these
            'impressions': 'sum',
            'clicks': 'sum',
            'cost': 'sum',
            'conversions': 'sum',
            'conversions_value': 'sum',
            'video_views': 'sum',
            'interactions': 'sum',
            'all_conversions': 'sum',
            
            # Average these
            'ctr': 'mean',
            'avg_cpc': 'mean',
            'avg_cpm': 'mean',
            'video_25_rate': 'mean',
            'video_50_rate': 'mean',
            'video_75_rate': 'mean',
            'video_100_rate': 'mean',
            
            # Keep first (representative)
            'video_title': 'first',
            'campaign_name': 'first',
            'campaign_id': 'first',
            'campaign_status': 'first',
            'campaign_type': 'first',
            'campaign_subtype': 'first',
            'campaign_start_date': 'first',
            'campaign_end_date': 'first',
            'package_id': 'first',
            'app_store': 'first',
            'ad_group_name': 'first',
            'ad_group_id': 'first',
            'ad_group_status': 'first',
            'ad_id': 'first',
            'ad_status': 'first',
            'account_name': lambda x: ', '.join(sorted(set(str(v) for v in x if v))),
            'account_id': lambda x: ', '.join(sorted(set(str(v) for v in x if v))),
            # Best overall statuses - handle UNKNOWN properly
            'status': lambda x: 'RUNNING' if 'RUNNING' in x.values else ('PAUSED' if 'PAUSED' in x.values else 'REMOVED'),
            'approval_status': lambda x: (
                'DISAPPROVED' if 'DISAPPROVED' in x.values 
                else ('APPROVED' if 'APPROVED' in x.values 
                else ('APPROVED LIMITED' if 'APPROVED LIMITED' in x.values
                else x.iloc[0]))  # Keep original if all same (including UNKNOWN)
            ),
            'review_status': lambda x: (
                'PENDING' if 'PENDING' in x.values 
                else ('REVIEWED' if 'REVIEWED' in x.values 
                else x.iloc[0])  # Keep original if all same (including UNKNOWN)
            )
        }
        
        df_agg = df_ads.groupby('youtube_url').agg(agg_dict).reset_index()
        
        logger.info(f"📊 Aggregated to {len(df_agg)} unique videos")
        
        status_counts = df_agg['status'].value_counts().to_dict()
        logger.info(f"   Status: {status_counts}")
        
        # Format numbers with currency signs and percentages for better readability
        def format_number(val):
            """Format number with commas (e.g., 1,234,567)"""
            try:
                return f"{int(val):,}"
            except:
                return str(val)
        
        def format_currency(val):
            """Format as currency with $ sign (e.g., $1,234.56)"""
            try:
                return f"${float(val):,.2f}"
            except:
                return str(val)
        
        def format_percentage(val):
            """Format as percentage with % sign (e.g., 12.34%)"""
            try:
                return f"{float(val) * 100:.2f}%"
            except:
                return str(val)
        
        def format_decimal(val):
            """Format decimal number (e.g., 12.34)"""
            try:
                return f"{float(val):.2f}"
            except:
                return str(val)
        
        # Apply formatting
        df_agg['impressions'] = df_agg['impressions'].apply(format_number)
        df_agg['clicks'] = df_agg['clicks'].apply(format_number)
        df_agg['cost'] = df_agg['cost'].apply(format_currency)
        df_agg['conversions'] = df_agg['conversions'].apply(format_decimal)
        df_agg['conversions_value'] = df_agg['conversions_value'].apply(format_currency)
        df_agg['video_views'] = df_agg['video_views'].apply(format_number)
        df_agg['interactions'] = df_agg['interactions'].apply(format_number)
        df_agg['all_conversions'] = df_agg['all_conversions'].apply(format_decimal)
        
        # Format rates with % sign
        df_agg['ctr'] = df_agg['ctr'].apply(format_percentage)
        # For CPC and CPM, show as plain decimal without $ sign per user request
        df_agg['avg_cpc'] = df_agg['avg_cpc'].apply(format_decimal)
        df_agg['avg_cpm'] = df_agg['avg_cpm'].apply(format_decimal)
        df_agg['video_25_rate'] = df_agg['video_25_rate'].apply(format_percentage)
        df_agg['video_50_rate'] = df_agg['video_50_rate'].apply(format_percentage)
        df_agg['video_75_rate'] = df_agg['video_75_rate'].apply(format_percentage)
        df_agg['video_100_rate'] = df_agg['video_100_rate'].apply(format_percentage)
        
        logger.info("✓ Applied formatting (Plain decimals for CPC/CPM, $ for costs, % for rates)")
        
        # Prepare for upload - rename columns with google_ads_ prefix
        rename_map = {
            'impressions': 'google_ads_impressions',
            'clicks': 'google_ads_clicks',
            'cost': 'google_ads_cost',
            'conversions': 'google_ads_conversions',
            'conversions_value': 'google_ads_conversions_value',
            'video_views': 'google_ads_video_views',
            'interactions': 'google_ads_interactions',
            'all_conversions': 'google_ads_all_conversions',
            'ctr': 'google_ads_ctr',
            'avg_cpc': 'google_ads_avg_cpc',
            'avg_cpm': 'google_ads_avg_cpm',
            'video_25_rate': 'google_ads_video_25_rate',
            'video_50_rate': 'google_ads_video_50_rate',
            'video_75_rate': 'google_ads_video_75_rate',
            'video_100_rate': 'google_ads_video_100_rate',
            'campaign_name': 'google_ads_campaign_name',
            'campaign_id': 'google_ads_campaign_id',
            'campaign_status': 'google_ads_campaign_status',
            'campaign_type': 'google_ads_campaign_type',
            'campaign_subtype': 'google_ads_campaign_subtype',
            'campaign_start_date': 'google_ads_campaign_start_date',
            'campaign_end_date': 'google_ads_campaign_end_date',
            'package_id': 'google_ads_package_id',
            'app_store': 'google_ads_app_store',
            'ad_group_name': 'google_ads_ad_group_name',
            'ad_group_id': 'google_ads_ad_group_id',
            'ad_group_status': 'google_ads_ad_group_status',
            'ad_id': 'google_ads_ad_id',
            'ad_status': 'google_ads_ad_status',
            'account_name': 'google_ads_account_name',
            'account_id': 'google_ads_account_id',
            'status': 'google_ads_status',
            'approval_status': 'google_ads_approval_status',
            'review_status': 'google_ads_review_status'
        }
        
        df_upload = df_agg.rename(columns=rename_map)
        
        # Drop columns not in rename_map (like video_title, youtube_id which we don't need)
        cols_to_keep = ['youtube_url'] + list(rename_map.values())
        df_upload = df_upload[[c for c in cols_to_keep if c in df_upload.columns]]
        
        # Upload to temp table then MERGE
        logger.info("💾 Uploading to temp table...")
        
        temp_table_id = f"{self.bq_client.project}.{self.dataset_id}.temp_google_ads_stats"
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = self.bq_client.load_table_from_dataframe(df_upload, temp_table_id, job_config=job_config)
        job.result()
        
        logger.info("💾 Merging into main table...")
        
        # Build dynamic SET clause from column names
        set_clauses = []
        for col in df_upload.columns:
            if col != 'youtube_url':
                set_clauses.append(f"T.{col} = S.{col}")
        set_clauses.append("T.google_ads_last_sync = CURRENT_TIMESTAMP()")
        
        merge_query = f"""
            MERGE `{self.full_table_id}` T
            USING `{temp_table_id}` S
            ON T.youtube_url = S.youtube_url
            WHEN MATCHED THEN
                UPDATE SET {', '.join(set_clauses)}
        """
        
        self.bq_client.query(merge_query).result()
        
        # Clean up temp table
        self.bq_client.delete_table(temp_table_id, not_found_ok=True)
        
        # Set status to PENDING for videos NOT in Google Ads (no status yet)
        logger.info("📝 Setting PENDING status for videos not in Google Ads...")
        pending_query = f"""
            UPDATE `{self.full_table_id}`
            SET google_ads_status = 'PENDING'
            WHERE google_ads_status IS NULL OR google_ads_status = ''
        """
        self.bq_client.query(pending_query).result()
        
        logger.info(f"✓ Updated {len(df_agg)} videos with Google Ads data")
        logger.info("✓ Videos not in Google Ads marked as PENDING")
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
