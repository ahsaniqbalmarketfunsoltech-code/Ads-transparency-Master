"""
Google Ads Video Performance Sync
Fetches video performance stats from Google Ads and syncs to BigQuery.
Uses google-ads library with proper gRPC configuration.
"""

import os
import json
import logging
import base64
from datetime import datetime
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

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

        # Google Ads Config
        self.ads_config = {
            "client_id": os.getenv('GOOGLE_ADS_CLIENT_ID'),
            "client_secret": os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
            "refresh_token": os.getenv('GOOGLE_ADS_REFRESH_TOKEN'),
            "developer_token": os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
            "use_proto_plus": True
        }
        
        login_customer_id = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
        if login_customer_id:
            self.ads_config["login_customer_id"] = login_customer_id.replace("-", "")
        
        logger.info(f"Initializing Google Ads client...")
        logger.info(f"  Developer token: {self.ads_config['developer_token'][:10]}...")
        logger.info(f"  Client ID: {self.ads_config['client_id'][:20]}...")
        
        try:
            self.ads_client = GoogleAdsClient.load_from_dict(self.ads_config)
            logger.info("  Google Ads client initialized successfully!")
        except Exception as e:
            logger.error(f"  Failed to initialize Google Ads client: {e}")
            raise

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
        """Query Google Ads for video stats"""
        ga_service = self.ads_client.get_service("GoogleAdsService")
        stats = []
        
        # Simple query for campaign assets with YouTube videos
        query = """
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
              AND metrics.impressions > 0
        """
        
        try:
            logger.info(f"    Querying campaign_asset for customer {customer_id}...")
            response = ga_service.search(customer_id=customer_id, query=query)
            
            for row in response:
                youtube_id = row.asset.youtube_video_asset.youtube_video_id
                if not youtube_id:
                    continue
                    
                stats.append({
                    'youtube_id': youtube_id,
                    'package_id': row.campaign.app_campaign_setting.app_id or 'N/A',
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name,
                    'impressions': row.metrics.impressions,
                    'clicks': row.metrics.clicks,
                    'cost': row.metrics.cost_micros / 1000000.0,
                    'account_id': str(row.customer.id),
                    'account_name': row.customer.descriptive_name,
                    'ad_group_id': 'N/A',
                    'ad_group_name': 'N/A'
                })
            
            logger.info(f"    Found {len(stats)} video stats")
            
        except GoogleAdsException as ex:
            logger.error(f"    Google Ads API error for {customer_id}:")
            for error in ex.failure.errors:
                logger.error(f"      Error: {error.message}")
                if error.location:
                    for field_error in error.location.field_path_elements:
                        logger.error(f"        Field: {field_error.field_name}")
        except Exception as e:
            logger.error(f"    Error querying customer {customer_id}: {e}")
        
        return stats

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
