import os
import json
import logging
import time
from datetime import datetime, timezone
from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
import pandas as pd
import base64

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        # We expect a JSON string for GOOGLE_ADS_CONFIG or individual env vars
        ads_config_json = os.getenv('GOOGLE_ADS_CONFIG')
        if ads_config_json:
            self.ads_config = json.loads(ads_config_json)
        else:
            self.ads_config = {
                "client_id": os.getenv('GOOGLE_ADS_CLIENT_ID'),
                "client_secret": os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
                "refresh_token": os.getenv('GOOGLE_ADS_REFRESH_TOKEN'),
                "developer_token": os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
                "use_proto_plus": True
            }
            if os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID'):
                self.ads_config["login_customer_id"] = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')

        self.ads_client = GoogleAdsClient.load_from_dict(self.ads_config)

    def hex_to_youtube_id(self, hex_id):
        """Standard conversion used in clean_data.py"""
        if not hex_id or len(hex_id) < 10: return None
        try:
            video_bytes = bytes.fromhex(hex_id.strip().lower())
            b64 = base64.b64encode(video_bytes).decode('utf-8')
            return b64.replace('+', '-').replace('/', '_').rstrip('=')
        except:
            return None

    def get_client_accounts(self, customer_id):
        """Recursively find all client accounts under a manager account"""
        ga_service = self.ads_client.get_service("GoogleAdsService")
        query = """
            SELECT
              customer_client.client_customer,
              customer_client.level,
              customer_client.manager,
              customer_client.descriptive_name,
              customer_client.id
            FROM customer_client
            WHERE customer_client.level <= 1
              AND customer_client.status = 'ENABLED'
        """
        
        client_ids = []
        try:
            search_request = self.ads_client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            response = ga_service.search(request=search_request)
            
            for row in response:
                client = row.customer_client
                if not client.manager:
                    client_ids.append(str(client.id))
                elif client.level > 0:
                    # It's a sub-manager, we could recurse if needed, but level 1 usually covers it
                    pass
        except Exception as e:
            logger.warning(f"Error listing clients for manager {customer_id}: {e}")
            
        return client_ids

    def get_accessible_customers(self):
        """Fetch all client account IDs, handling Manager accounts"""
        customer_service = self.ads_client.get_service("CustomerService")
        try:
            customer_resource_names = customer_service.list_accessible_customers()
        except Exception as e:
            logger.error(f"Failed to list accessible customers: {e}")
            return []
        
        all_client_ids = set()
        for resource_name in customer_resource_names.resource_names:
            cid = resource_name.split("/")[-1]
            # Check if this is a manager or a client
            clients = self.get_client_accounts(cid)
            if clients:
                all_client_ids.update(clients)
            else:
                # Might be a single client account
                all_client_ids.add(cid)
        
        logger.info(f"Total client accounts to process: {len(all_client_ids)}")
        return list(all_client_ids)

    def get_video_stats(self, customer_id):
        """Query Google Ads for video stats including package ID (app_id)"""
        ga_service = self.ads_client.get_service("GoogleAdsService")
        stats = []
        
        # Query: Assets linked to campaigns or ad groups
        # This covers App Campaigns and modern Video Ads (Responsive Video Ads)
        query = """
            SELECT
              asset.youtube_video_asset.youtube_video_id,
              campaign.app_campaign_settings.app_id,
              campaign.id,
              campaign.name,
              ad_group.id,
              ad_group.name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.video_views,
              customer.id,
              customer.descriptive_name
            FROM ad_group_asset
            WHERE asset.type = 'YOUTUBE_VIDEO'
              AND metrics.impressions > 0
        """
        # Note: If no assets are at ad_group level, we also check campaign level
        query_campaign = """
            SELECT
              asset.youtube_video_asset.youtube_video_id,
              campaign.app_campaign_settings.app_id,
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
        
        for q in [query, query_campaign]:
            try:
                search_request = self.ads_client.get_type("SearchGoogleAdsRequest")
                search_request.customer_id = customer_id
                search_request.query = q
                
                response = ga_service.search(request=search_request)
                for row in response:
                    pkg_id = row.campaign.app_campaign_settings.app_id
                    
                    stat = {
                        'youtube_id': row.asset.youtube_video_asset.youtube_video_id,
                        'package_id': pkg_id if pkg_id else 'N/A',
                        'campaign_id': str(row.campaign.id),
                        'campaign_name': row.campaign.name,
                        'impressions': row.metrics.impressions,
                        'clicks': row.metrics.clicks,
                        'cost': row.metrics.cost_micros / 1000000.0,
                        'conversions': row.metrics.conversions,
                        'video_views': row.metrics.video_views,
                        'account_id': str(row.customer.id),
                        'account_name': row.customer.descriptive_name
                    }
                    
                    # Add ad group info if available (only in the first query)
                    if hasattr(row, 'ad_group'):
                        stat['ad_group_id'] = str(row.ad_group.id)
                        stat['ad_group_name'] = row.ad_group.name
                    else:
                        stat['ad_group_id'] = 'N/A'
                        stat['ad_group_name'] = 'N/A'
                        
                    stats.append(stat)
            except Exception as e:
                logger.debug(f"Query failed for customer {customer_id}: {e}")

        return stats

    def init_bigquery(self):
        """Ensure Dataset exists"""
        dataset_ref = self.bq_client.dataset(self.dataset_id)
        try:
            self.bq_client.get_dataset(dataset_ref)
        except Exception:
            logger.info(f"Creating dataset '{self.dataset_id}'...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.bq_client.create_dataset(dataset)

    def run_sync(self):
        logger.info("Starting Google Ads Video Sync...")
        self.init_bigquery()
        
        # 1. Fetch existing videos from BigQuery to get the hex -> youtube_id mapping
        try:
            query = f"SELECT video_id, youtube_url FROM `{self.full_table_id}`"
            df_bq = self.bq_client.query(query).to_dataframe()
            logger.info(f"Loaded {len(df_bq)} videos from BigQuery.")
        except Exception as e:
            logger.error(f"Could not read from BigQuery: {e}")
            return

        # Build mapping: youtube_id -> video_id (hex)
        yt_to_hex = {}
        for _, row in df_bq.iterrows():
            hex_id = row['video_id']
            yt_id = self.hex_to_youtube_id(hex_id)
            if yt_id:
                yt_to_hex[yt_id] = hex_id

        # 2. Iterate through accounts and fetch stats
        all_stats = []
        customer_ids = self.get_accessible_customers()
        
        for cid in customer_ids:
            logger.info(f"Processing Customer: {cid}")
            account_stats = self.get_video_stats(cid)
            all_stats.extend(account_stats)

        if not all_stats:
            logger.info("No video stats found in Google Ads.")
            return

        # 3. Aggregate stats by (youtube_id, package_id)
        df_ads = pd.DataFrame(all_stats)
        
        # Deduplicate: if a row has same (account, campaign, ad_group, youtube_id, package_id), it's a duplicate
        # We also want to avoid double counting if a video is reported at both Campaign and Ad Group level.
        # Strategy: Prioritize Ad Group level stats if available, otherwise Campaign level.
        # However, for simplicity and accuracy, we'll just drop exact duplicates first.
        df_ads = df_ads.drop_duplicates(subset=['account_id', 'campaign_id', 'ad_group_id', 'youtube_id', 'package_id'])
        
        # Filter: only keep stats for videos we have in our database
        df_ads['video_id'] = df_ads['youtube_id'].map(yt_to_hex)
        df_ads = df_ads.dropna(subset=['video_id'])
        
        if df_ads.empty:
            logger.info("None of the videos in Google Ads match the BigQuery database.")
            return

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

        # 4. Upload the full aggregated dataset back to BigQuery
        stats_table_id = f"{self.bq_client.project}.{self.dataset_id}.video_performance_stats"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        
        logger.info(f"Uploading {len(df_agg)} aggregated stats rows to {stats_table_id}...")
        job = self.bq_client.load_table_from_dataframe(df_agg, stats_table_id, job_config=job_config)
        job.result()
        
        # 5. Create Comparison View
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
            logger.info(f"✓ Comparison view created: {view_id}")
        except Exception as e:
            logger.warning(f"Could not create comparison view: {e}")

if __name__ == "__main__":
    try:
        sync = GoogleAdsVideoSync()
        sync.run_sync()
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
