"""
Google Ads Video Performance Sync (REST API Version)
Uses direct REST API calls instead of the google-ads Python library to avoid gRPC issues.
"""

import os
import json
import logging
import base64
from datetime import datetime
from pathlib import Path
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2.credentials import Credentials

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

        # Google Ads REST API Config
        self.developer_token = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
        self.client_id = os.getenv('GOOGLE_ADS_CLIENT_ID')
        self.client_secret = os.getenv('GOOGLE_ADS_CLIENT_SECRET')
        self.refresh_token = os.getenv('GOOGLE_ADS_REFRESH_TOKEN')
        
        if not all([self.developer_token, self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError("Missing Google Ads credentials")
        
        # Get access token
        self.access_token = self._get_access_token()
        
        # API base URL - use v17 which is stable
        self.api_version = "v17"
        self.base_url = f"https://googleads.googleapis.com/{self.api_version}"

    def _get_access_token(self):
        """Get access token using refresh token"""
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }
        response = requests.post(token_url, data=data)
        if response.status_code != 200:
            logger.error(f"Failed to get access token: {response.text}")
            raise Exception("Failed to authenticate with Google")
        
        token = response.json()["access_token"]
        logger.info(f"Access token obtained successfully (length: {len(token)})")
        logger.info(f"Developer token: {self.developer_token[:10]}..." if self.developer_token else "Developer token: MISSING!")
        return token


    def _make_request(self, endpoint, method="GET", data=None, customer_id=None):
        """Make REST API request to Google Ads"""
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "developer-token": self.developer_token,
            "Content-Type": "application/json"
        }
        
        if customer_id:
            headers["login-customer-id"] = customer_id
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(url, headers=headers)
            else:
                response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.debug(f"API Error ({response.status_code}): {response.text[:500]}")
                return None
        except Exception as e:
            logger.debug(f"Request failed: {e}")
            return None

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

    def search_google_ads(self, customer_id, query):
        """Execute a GAQL query using REST API"""
        # Use the regular search endpoint (not stream)
        url = f"https://googleads.googleapis.com/{self.api_version}/customers/{customer_id}/googleAds:search"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "developer-token": self.developer_token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # For the regular search endpoint, we need pageSize
        data = {
            "query": query,
            "pageSize": 10000
        }
        
        logger.info(f"    Calling: {url}")
        
        try:
            response = requests.post(url, headers=headers, json=data)
            
            logger.info(f"    API Response Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                all_results = result.get("results", [])
                logger.info(f"    Rows returned: {len(all_results)}")
                return all_results
            else:
                # Log the actual error message
                try:
                    error_json = response.json()
                    error_msg = json.dumps(error_json, indent=2)[:1500]
                except:
                    error_msg = response.text[:1500] if response.text else "No error message"
                logger.warning(f"    API Error: {error_msg}")
                return []
        except Exception as e:
            logger.error(f"    Request exception: {e}")
            return []

    def get_video_stats(self, customer_id):
        """Query Google Ads for video stats using REST API"""
        stats = []
        
        # Query for campaign-level assets
        query = """
            SELECT
              asset.youtube_video_asset.youtube_video_id,
              campaign.app_campaign_setting.app_id,
              campaign.id,
              campaign.name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.video_views,
              customer.id,
              customer.descriptive_name
            FROM campaign_asset
            WHERE asset.type = 'YOUTUBE_VIDEO'
              AND metrics.impressions > 0
        """
        
        results = self.search_google_ads(customer_id, query)
        
        for row in results:
            try:
                asset = row.get("asset", {})
                campaign = row.get("campaign", {})
                metrics = row.get("metrics", {})
                customer = row.get("customer", {})
                
                youtube_asset = asset.get("youtubeVideoAsset", {})
                app_settings = campaign.get("appCampaignSetting", {})
                
                youtube_id = youtube_asset.get("youtubeVideoId")
                if not youtube_id:
                    continue
                
                stats.append({
                    'youtube_id': youtube_id,
                    'package_id': app_settings.get("appId", "N/A"),
                    'campaign_id': str(campaign.get("id", "")),
                    'campaign_name': campaign.get("name", ""),
                    'impressions': int(metrics.get("impressions", 0)),
                    'clicks': int(metrics.get("clicks", 0)),
                    'cost': float(metrics.get("costMicros", 0)) / 1000000.0,
                    'conversions': float(metrics.get("conversions", 0)),
                    'video_views': int(metrics.get("videoViews", 0)),
                    'account_id': str(customer.get("id", "")),
                    'account_name': customer.get("descriptiveName", ""),
                    'ad_group_id': 'N/A',
                    'ad_group_name': 'N/A'
                })
            except Exception as e:
                logger.debug(f"Error parsing row: {e}")
                continue
        
        logger.info(f"  Found {len(stats)} video stats for customer {customer_id}")
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
            'conversions': 'sum',
            'video_views': 'sum',
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
              s.conversions as google_ads_conversions,
              s.video_views as google_ads_youtube_views,
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
