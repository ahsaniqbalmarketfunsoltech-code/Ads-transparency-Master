#!/usr/bin/env python3
"""
Text Ads Data Cleaner with BigQuery Sync
Reads from 'Text Ads data' Google Sheet, filters valid Play Store links,
and syncs specific columns (A, C, D, E) to Google BigQuery.
"""

import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from google.api_core import exceptions as bq_exceptions
import pandas as pd

# Setup logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/clean_text_ads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
MASTER_SHEET_ID = '1yq2UwI94lwfYPY86CFwGbBsm3kpdqKrefYgrw3lEAwk'
TAB_NAME = 'Text Ads data'
DEFAULT_DATASET = 'ads_data_staging'
DEFAULT_TABLE = 'clean_text_ads'

class TextAdsCleaner:
    def __init__(self, credentials_json, sheet_id):
        """Initialize with service account credentials for Sheets and BigQuery"""
        self.sheet_id = sheet_id
        
        try:
            creds_dict = json.loads(credentials_json)
            self.project_id = creds_dict.get('project_id')
            
            # Scopes for Sheets, Drive, and BigQuery
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/bigquery'
            ]
            self.creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.gs_client = gspread.authorize(self.creds)
            
            # BigQuery Client
            self.bq_client = bigquery.Client(credentials=self.creds, project=self.project_id)
            
            # BQ Config
            self.dataset_id = os.getenv('BIGQUERY_DATASET', DEFAULT_DATASET)
            self.table_id = os.getenv('BIGQUERY_TABLE', DEFAULT_TABLE)
            self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            
            logger.info(f"✓ Authenticated. BQ Target: {self.full_table_id}")
            
        except Exception as e:
            logger.error(f"Authentication Failed: {e}")
            raise

    def init_bigquery(self):
        """Ensure Dataset and Table exist"""
        # 1. Ensure Dataset exists
        dataset_ref = self.bq_client.dataset(self.dataset_id)
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"✓ Dataset '{self.dataset_id}' exists")
        except bq_exceptions.NotFound:
            logger.info(f"Creating dataset '{self.dataset_id}'...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.bq_client.create_dataset(dataset)

        # 2. Ensure Table exists with correct schema
        table_ref = dataset_ref.table(self.table_id)
        schema = [
            bigquery.SchemaField("advertiser_name", "STRING"),
            bigquery.SchemaField("app_link", "STRING"),
            bigquery.SchemaField("app_name", "STRING"),
            bigquery.SchemaField("app_headline", "STRING"),
            bigquery.SchemaField("last_updated", "TIMESTAMP"),
        ]
        try:
            self.bq_client.get_table(table_ref)
            logger.info(f"✓ Table '{self.table_id}' exists")
        except bq_exceptions.NotFound:
            logger.info(f"Creating table '{self.table_id}'...")
            table = bigquery.Table(table_ref, schema=schema)
            # Partitioning by upload time for optimization
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="last_updated"
            )
            self.bq_client.create_table(table)

    def read_sheet_data(self):
        """Read data from the specified tab in Google Sheets"""
        logger.info(f"Reading '{TAB_NAME}' from sheet {self.sheet_id}...")
        try:
            spreadsheet = self.gs_client.open_by_key(self.sheet_id)
            worksheet = spreadsheet.worksheet(TAB_NAME)
            rows = worksheet.get_all_values()
            
            if len(rows) < 2:
                logger.warning("No data found in sheet.")
                return []
            
            return rows
        except Exception as e:
            logger.error(f"Failed to read sheet: {e}")
            return []

    def get_existing_links(self):
        """Get existing app_links from BigQuery to avoid duplicates"""
        query = f"SELECT DISTINCT app_link FROM `{self.full_table_id}`"
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            return {row.app_link for row in results}
        except Exception as e:
            logger.warning(f"Could not query BigQuery for existing links (table might be new): {e}")
            return set()

    def process_data(self, rows, existing_links):
        """Filter rows where column C contains Play Store link, select A, C, D, E, and exclude existing links"""
        if not rows:
            return pd.DataFrame()

        data_rows = rows[1:]
        
        processed_data = []
        for row in data_rows:
            # Column mapping (0-indexed):
            # A: 0, B: 1, C: 2, D: 3, E: 4
            
            if len(row) < 5:
                continue
                
            advertiser = row[0].strip()
            app_link = row[2].strip()
            app_name = row[3].strip()
            app_headline = row[4].strip()
            
            # Filter: Column C must have Play Store link AND not already be in BigQuery
            if 'play.google.com' in app_link and app_link not in existing_links:
                processed_data.append({
                    'advertiser_name': advertiser,
                    'app_link': app_link,
                    'app_name': app_name,
                    'app_headline': app_headline,
                    'last_updated': datetime.now(timezone.utc)
                })
        
        df = pd.DataFrame(processed_data)
        if not df.empty:
            df = df.drop_duplicates(subset=['app_link'])
            
        logger.info(f"Found {len(df)} NEW rows after filtering.")
        return df

    def upload_to_bq(self, df):
        """Upload DataFrame to BigQuery using WRITE_APPEND"""
        if df.empty:
            logger.info("No NEW data to upload.")
            return

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
        
        logger.info(f"Appending {len(df)} new rows to {self.full_table_id}...")
        job = self.bq_client.load_table_from_dataframe(df, self.full_table_id, job_config=job_config)
        job.result()
        logger.info("✓ BigQuery Append Complete!")

    def run(self):
        """Main execution flow"""
        self.init_bigquery()
        existing_links = self.get_existing_links()
        rows = self.read_sheet_data()
        df = self.process_data(rows, existing_links)
        self.upload_to_bq(df)

def main():
    # Load credentials and sheet ID from environment variables
    # This matches the structure used in clean_data.py
    creds = os.getenv('GOOGLE_CREDENTIALS')
    sheet_id = MASTER_SHEET_ID  # Using the specific sheet ID provided
    
    if not creds:
        # Fallback for local testing if needed
        json_path = r'c:\Users\ahsan iqbal\Downloads\ads-master-bigqeury-0447e1f55f8c.json'
        if os.path.exists(json_path):
            with open(json_path, 'r') as f:
                creds = f.read()
        else:
            logger.error("Missing GOOGLE_CREDENTIALS environment variable or local JSON file.")
            return

    cleaner = TextAdsCleaner(creds, sheet_id)
    cleaner.run()

if __name__ == '__main__':
    main()
