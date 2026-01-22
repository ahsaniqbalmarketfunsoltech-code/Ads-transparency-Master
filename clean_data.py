#!/usr/bin/env python3
"""
Google Sheets Data Cleaner with BigQuery Sync
Reads from 'Combined Data' Google Sheet, filters valid Play Store links, 
fetches YouTube stats, and syncs to Google BigQuery.
Runs on GitHub Actions every hour.
"""

import os
import json
import time
import logging
import traceback
import re
from datetime import datetime, timezone
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from google.api_core import exceptions as bq_exceptions
import pandas as pd
import numpy as np
import requests

# Setup logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/clean_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BATCH_SIZE = 50000
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY', 'AIzaSyCov_GZu8554LbvjERy2VCbRWnhUjIToZA')
YOUTUBE_API_URL = 'https://www.googleapis.com/youtube/v3/videos'

# BigQuery Defaults
DEFAULT_DATASET = 'ads_data_staging'
DEFAULT_TABLE = 'clean_ads_transparency'

class DataCleaner:
    def __init__(self, credentials_json, master_sheet_id):
        """Initialize with service account credentials for Sheets and BigQuery"""
        self.master_sheet_id = master_sheet_id
        self.youtube_cache = {}
        
        # Load Credentials
        try:
            creds_dict = json.loads(credentials_json)
            self.project_id = creds_dict.get('project_id')
            
            # Sheets Auth
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/bigquery'
            ]
            self.creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.client = gspread.authorize(self.creds)
            
            # BigQuery Auth
            self.bq_client = bigquery.Client(credentials=self.creds, project=self.project_id)
            
            # BQ Config
            self.dataset_id = os.getenv('BIGQUERY_DATASET', DEFAULT_DATASET)
            self.table_id = os.getenv('BIGQUERY_TABLE', DEFAULT_TABLE)
            self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            
            logger.info(f"✓ Authenticated. BQ Target: {self.full_table_id}")
            
        except Exception as e:
            logger.error(f"Auth Failed: {e}")
            raise

    def init_bigquery(self):
        """Ensure Dataset and Table exist"""
        # 1. Create Dataset
        dataset_ref = self.bq_client.dataset(self.dataset_id)
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"✓ Dataset '{self.dataset_id}' exists")
        except bq_exceptions.NotFound:
            logger.info(f"Creating dataset '{self.dataset_id}'...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Modify if needed
            try:
                self.bq_client.create_dataset(dataset)
            except bq_exceptions.Forbidden as e:
                logger.error(f"Permission Denied: Could not create dataset '{self.dataset_id}' in project '{self.project_id}'.")
                logger.error("Fix: Grant 'BigQuery User' or 'BigQuery Admin' role to the service account, or manually create the dataset in the Google Cloud Console.")
                raise e

        # 2. Create Table
        table_ref = dataset_ref.table(self.table_id)
        schema = [
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("youtube_url", "STRING"),
            bigquery.SchemaField("app_link", "STRING"),
            bigquery.SchemaField("app_name", "STRING"),
            bigquery.SchemaField("advertiser_name", "STRING"),
            bigquery.SchemaField("views", "INTEGER"),
            bigquery.SchemaField("upload_time", "STRING"),
            bigquery.SchemaField("last_updated", "TIMESTAMP"),
        ]
        
        try:
            self.bq_client.get_table(table_ref)
            logger.info(f"✓ Table '{self.table_id}' exists")
        except bq_exceptions.NotFound:
            logger.info(f"Creating table '{self.table_id}'...")
            table = bigquery.Table(table_ref, schema=schema)
            # Partition by timestamp for better performance/cost
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="last_updated"
            )
            self.bq_client.create_table(table)

    def extract_video_id(self, url):
        if not url: return None
        patterns = [
            r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
            r'youtu\.be/([a-zA-Z0-9_-]{11})',
            r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
            r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match: return match.group(1)
        return None

    def get_youtube_stats_batch(self, video_ids):
        """Fetch stats for list of video IDs"""
        if not video_ids: return {}
        
        # Deduplicate and check cache
        unique_ids = list(set(video_ids))
        uncached = [vid for vid in unique_ids if vid not in self.youtube_cache]
        
        if not uncached:
            return {vid: self.youtube_cache.get(vid, (None, None)) for vid in video_ids}

        logger.info(f"Fetching stats for {len(uncached)} videos from YouTube API...")
        logger.info(f"Sample IDs: {uncached[:5]}")
        
        # Mask API key for logging
        masked_key = f"{YOUTUBE_API_KEY[:4]}...{YOUTUBE_API_KEY[-4:]}" if YOUTUBE_API_KEY else "Missing"
        logger.info(f"Using YouTube API Key: {masked_key}")

        for i in range(0, len(uncached), 50):
            batch = uncached[i:i+50]
            try:
                params = {
                    'part': 'statistics,snippet',
                    'id': ','.join(batch),
                    'key': YOUTUBE_API_KEY
                }
                resp = requests.get(YOUTUBE_API_URL, params=params, timeout=30)
                
                if resp.status_code == 200:
                    data = resp.json()
                    found_ids = set()
                    for item in data.get('items', []):
                        vid = item['id']
                        stats = item.get('statistics', {})
                        views = int(stats.get('viewCount', 0))
                        
                        # Time ago
                        snippet = item.get('snippet', {})
                        pub_at = snippet.get('publishedAt')
                        time_ago = self.calculate_time_ago(pub_at) if pub_at else ""
                        
                        self.youtube_cache[vid] = (views, time_ago)
                        found_ids.add(vid)
                    
                    # Mark missing
                    for vid in batch:
                        if vid not in found_ids:
                            self.youtube_cache[vid] = (None, None)
                else:
                    logger.error(f"YouTube API Error: Status {resp.status_code}")
                    logger.error(f"Response: {resp.text}")
                    for vid in batch:
                        self.youtube_cache[vid] = (None, None)
                        
            except Exception as e:
                logger.error(f"Request Failure: {e}")
                for vid in batch:
                    self.youtube_cache[vid] = (None, None)
            
            time.sleep(0.1)
            
        return {vid: self.youtube_cache.get(vid, (None, None)) for vid in video_ids}

    def calculate_time_ago(self, published_at):
        try:
            dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            diff = now - dt
            days = diff.days
            if days < 1: return "Today"
            if days < 30: return f"{days} days ago"
            if days < 365: return f"{days // 30} months ago"
            return f"{days // 365} years ago"
        except:
            return ""

    def read_combined_sheet(self):
        """Read all raw data from 'Combined Data'"""
        logger.info("Reading 'Combined Data' from Sheet...")
        sheet = self.client.open_by_key(self.master_sheet_id).worksheet('Combined Data')
        # Expecting raw data. We assume columns are roughly:
        # 0: Advertiser, 1: YouTube URL, 2: App Link, 3: App Name, 4: VideoID (optional)
        rows = sheet.get_all_values()
        if len(rows) < 2: return []
        return rows[1:] # Skip header

    def get_bq_state(self):
        """Get ALL existing data from BQ for merge in Python"""
        query = f"""
            SELECT video_id, youtube_url, app_link, app_name, advertiser_name, views, upload_time, last_updated
            FROM `{self.full_table_id}` 
        """
        try:
            df = self.bq_client.query(query).to_dataframe()
            return df
        except Exception as e:
            logger.warning(f"Could not query BQ (maybe table empty): {e}")
            return pd.DataFrame(columns=['video_id', 'views'])

    def process_and_sync(self):
        self.init_bigquery()
        
        # 1. Read Sheet Data
        raw_rows = self.read_combined_sheet()
        logger.info(f"Sheet has {len(raw_rows)} rows.")
        
        # 2. Parse into DataFrame
        data = []
        for row in raw_rows:
            advertiser = row[0] if len(row) > 0 else ''
            url = row[1] if len(row) > 1 else ''
            app_link = row[2] if len(row) > 2 else ''
            app_name = row[3] if len(row) > 3 else ''
            
            if 'play.google.com' not in app_link:
                continue
                
            vid = self.extract_video_id(url)
            if not vid and len(row) > 4:
                vid = row[4]
                
            if vid:
                data.append({
                    'video_id': vid,
                    'youtube_url': url,
                    'app_link': app_link,
                    'app_name': app_name,
                    'advertiser_name': advertiser
                })
        
        if not data:
            logger.info("No valid data found in sheet.")
            return

        df_sheet = pd.DataFrame(data).drop_duplicates(subset=['video_id'])
        logger.info(f"Found {len(df_sheet)} unique valid videos in Sheet.")

        # 3. Get Existing Data from BigQuery
        df_bq = self.get_bq_state()
        
        # 4. Identify videos that need YouTube stats:
        #    a) NEW videos (not in BQ)
        #    b) INCOMPLETE videos (in BQ but views=0/null OR upload_time is empty)
        
        existing_ids = df_bq['video_id'].tolist() if not df_bq.empty else []
        
        # New videos from sheet
        df_new = df_sheet[~df_sheet['video_id'].isin(existing_ids)].copy()
        logger.info(f"New videos: {len(df_new)}")
        
        # Incomplete videos already in BQ (need retry)
        df_incomplete = pd.DataFrame()
        if not df_bq.empty:
            # Find rows where views is 0/null OR upload_time is empty/null
            incomplete_mask = (
                (df_bq['views'].isna()) | 
                (df_bq['views'] == 0) | 
                (df_bq['upload_time'].isna()) | 
                (df_bq['upload_time'] == '') |
                (df_bq['upload_time'] == 'None')
            )
            df_incomplete = df_bq[incomplete_mask].copy()
            logger.info(f"Incomplete videos (missing views/upload_time): {len(df_incomplete)}")
        
        # Combine: videos needing YouTube API fetch
        videos_to_fetch = []
        if not df_new.empty:
            videos_to_fetch.extend(df_new['video_id'].tolist())
        if not df_incomplete.empty:
            videos_to_fetch.extend(df_incomplete['video_id'].tolist())
        
        videos_to_fetch = list(set(videos_to_fetch))  # Deduplicate
        logger.info(f"Total videos to fetch YouTube stats for: {len(videos_to_fetch)}")
        
        if videos_to_fetch:
            # 5. Fetch YouTube Stats
            stats_map = self.get_youtube_stats_batch(videos_to_fetch)
            
            # Apply stats to NEW videos
            if not df_new.empty:
                df_new['views'] = df_new['video_id'].map(lambda x: stats_map.get(x, (0, ""))[0])
                df_new['upload_time'] = df_new['video_id'].map(lambda x: stats_map.get(x, (0, ""))[1])
                df_new['last_updated'] = datetime.now(timezone.utc)
            
            # Update stats for INCOMPLETE videos in BQ data
            if not df_incomplete.empty:
                for vid in df_incomplete['video_id'].tolist():
                    if vid in stats_map:
                        views, upload_time = stats_map[vid]
                        df_bq.loc[df_bq['video_id'] == vid, 'views'] = views if views else 0
                        df_bq.loc[df_bq['video_id'] == vid, 'upload_time'] = upload_time if upload_time else ''
                        df_bq.loc[df_bq['video_id'] == vid, 'last_updated'] = datetime.now(timezone.utc)
            
            # 6. Combine: existing BQ data (updated) + new videos
            if not df_bq.empty:
                df_bq['last_updated'] = pd.to_datetime(df_bq['last_updated'])
                if not df_new.empty:
                    df_final = pd.concat([df_bq, df_new], ignore_index=True)
                else:
                    df_final = df_bq
            else:
                df_final = df_new
        else:
            logger.info("No videos need YouTube stats. Nothing to update.")
            return

        # 7. Upload the full combined dataset back to BigQuery
        self.upload_to_bq(df_final)

    def upload_to_bq(self, df):
        """Uploads full DataFrame to BigQuery (Free Tier Compatible)"""
        if df.empty: return

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("video_id", "STRING"),
                bigquery.SchemaField("youtube_url", "STRING"),
                bigquery.SchemaField("app_link", "STRING"),
                bigquery.SchemaField("app_name", "STRING"),
                bigquery.SchemaField("advertiser_name", "STRING"),
                bigquery.SchemaField("views", "INTEGER"),
                bigquery.SchemaField("upload_time", "STRING"),
                bigquery.SchemaField("last_updated", "TIMESTAMP"),
            ],
            write_disposition="WRITE_TRUNCATE" # Overwrites table with the full combined data
        )
        
        logger.info(f"Uploading {len(df)} total rows to {self.full_table_id}...")
        job = self.bq_client.load_table_from_dataframe(df, self.full_table_id, job_config=job_config)
        job.result()
        logger.info("✓ BigQuery Sync Complete!")

def main():
    creds = os.getenv('GOOGLE_CREDENTIALS')
    sheet_id = os.getenv('MASTER_SPREADSHEET_ID')
    
    if not creds or not sheet_id:
        logger.error("Missing Environment Variables")
        return
        
    cleaner = DataCleaner(creds, sheet_id)
    cleaner.process_and_sync()

if __name__ == '__main__':
    main()
