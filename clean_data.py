#!/usr/bin/env python3
"""
Google Sheets Data Cleaner
Reads from 'Combined Data' tab, filters valid Play Store links, writes to 'Clean data' tab
Runs on GitHub Actions every hour (after combine-sheets workflow)
"""

import os
import json
import time
import logging
import traceback
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np

# Setup logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/clean_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BATCH_SIZE = 50000  # Rows to write per batch
MAX_RETRIES = 5
RETRY_DELAY = 3  # seconds

# Column mapping from Combined Data (0-based indices)
# Source columns in Combined Data:
#   0: Advertiser Name
#   1: Ads URL (SKIP)
#   2: App Link ← Filter by this (must be valid Play Store link)
#   3: App Name
#   4: Video ID

# Columns to KEEP (indices in source data) - in the order for Clean data output:
# Output: Video ID, App Link, App Name, Advertiser Name
COLUMNS_TO_KEEP = [4, 2, 3, 0]  # Video ID, App Link, App Name, Advertiser Name

# App Link column index in SOURCE data (for filtering)
APP_LINK_COLUMN_INDEX = 2

class DataCleaner:
    def __init__(self, credentials_json, master_sheet_id):
        """Initialize with service account credentials"""
        self.master_sheet_id = master_sheet_id
        
        # Authenticate
        creds_dict = json.loads(credentials_json)
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        self.client = gspread.authorize(creds)
        
        logger.info("✓ Authenticated with Google Sheets API")
    
    def is_valid_playstore_link(self, value):
        """Check if value is a valid Play Store link"""
        if not value or not isinstance(value, str):
            return False
        
        value = value.strip().lower()
        
        # Must contain play.google.com
        if 'play.google.com' not in value:
            return False
        
        # Reject common invalid values
        invalid_values = [
            'not_found', 'not found', 'notfound',
            'n/a', 'na', 'none', 'null', 'undefined',
            'error', 'blocked', 'failed',
            ''
        ]
        
        if value in invalid_values:
            return False
        
        return True
    
    def read_combined_data(self):
        """Read data from 'Combined Data' tab"""
        try:
            logger.info("Reading data from 'Combined Data' tab...")
            
            master = self.client.open_by_key(self.master_sheet_id)
            combined_sheet = master.worksheet('Combined Data')
            
            # Get all values (skip header row 1)
            all_data = combined_sheet.get_all_values()
            
            if not all_data:
                logger.warning("No data found in 'Combined Data' tab")
                return None
            
            # Skip header row (row 1)
            data_rows = all_data[1:] if len(all_data) > 1 else []
            
            logger.info(f"✓ Read {len(data_rows)} data rows from 'Combined Data'")
            return data_rows
            
        except Exception as e:
            logger.error(f"Failed to read combined data: {e}")
            logger.debug(traceback.format_exc())
            raise
    
    def clean_data(self, data_rows):
        """Filter rows to keep only those with valid Play Store links, remove duplicate Video IDs, and map to output columns"""
        if not data_rows:
            return []
        
        logger.info(f"Cleaning {len(data_rows)} rows...")
        
        # Output column mapping (starting from column B):
        # Clean data headers:
        #   B: Video ID      → output position 0
        #   C: Base 64       → output position 1 (empty)
        #   D: Full Youtube  → output position 2 (empty)
        #   E: App Link      → output position 3
        #   F: App Name      → output position 4
        #   G: Advertiser    → output position 5
        
        cleaned_rows = []
        removed_invalid = 0
        removed_duplicate = 0
        seen_video_ids = set()
        
        for row in data_rows:
            # Get App Link value for filtering (source column index 2)
            app_link = row[APP_LINK_COLUMN_INDEX] if len(row) > APP_LINK_COLUMN_INDEX else ''
            
            if not self.is_valid_playstore_link(app_link):
                removed_invalid += 1
                continue
            
            # Get Video ID for duplicate check (source column index 4)
            video_id = row[4] if len(row) > 4 else ''
            
            # Skip if we've already seen this Video ID
            if video_id and video_id in seen_video_ids:
                removed_duplicate += 1
                continue
            
            # Mark this Video ID as seen
            if video_id:
                seen_video_ids.add(video_id)
            
            # Build output row with correct column positions
            # Source indices: 0=Advertiser, 1=Ads URL, 2=App Link, 3=App Name, 4=Video ID
            
            app_link_val = row[2] if len(row) > 2 else ''
            app_name = row[3] if len(row) > 3 else ''
            advertiser = row[0] if len(row) > 0 else ''
            
            # Output row for columns B through G (6 columns):
            # B=Video ID, C=empty, D=empty, E=App Link, F=App Name, G=Advertiser
            output_row = [
                video_id,      # B: Video ID
                '',            # C: Base 64 (empty)
                '',            # D: Full Youtube (empty)
                app_link_val,  # E: App Link
                app_name,      # F: App Name
                advertiser     # G: Advertiser Name
            ]
            
            cleaned_rows.append(output_row)
        
        logger.info(f"✓ Cleaning complete:")
        logger.info(f"  - Kept: {len(cleaned_rows)} unique valid rows")
        logger.info(f"  - Removed invalid App Links: {removed_invalid}")
        logger.info(f"  - Removed duplicate Video IDs: {removed_duplicate}")
        logger.info(f"  - Output: B=Video ID, E=App Link, F=App Name, G=Advertiser")
        
        return cleaned_rows
    
    def get_existing_video_ids(self):
        """Read existing Video IDs from Clean data sheet"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            try:
                clean_sheet = master.worksheet('Clean data')
                # Get all values from column B (Video ID column)
                all_data = clean_sheet.get_all_values()
                
                # Skip header row, get Video IDs from column B (index 1, since we read from A)
                # But our data starts at column B, so when reading all values, B is index 1
                existing_ids = set()
                for row in all_data[1:]:  # Skip header
                    if len(row) > 1 and row[1]:  # Column B is index 1
                        existing_ids.add(row[1])
                
                logger.info(f"✓ Found {len(existing_ids)} existing Video IDs in Clean data")
                return existing_ids, len(all_data)  # Return IDs and current row count
                
            except gspread.WorksheetNotFound:
                logger.info("Clean data sheet not found, will create new one")
                return set(), 1  # No existing IDs, start after header row
                
        except Exception as e:
            logger.error(f"Failed to read existing data: {e}")
            logger.debug(traceback.format_exc())
            return set(), 1
    
    def write_to_clean_data(self, data_rows, existing_ids, current_row_count):
        """Append new cleaned data to 'Clean data' tab (incremental, no clearing)"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            # Filter out rows that already exist in Clean data
            new_rows = []
            for row in data_rows:
                video_id = row[0]  # Video ID is first column in output
                if video_id and video_id not in existing_ids:
                    new_rows.append(row)
            
            if not new_rows:
                logger.info("✓ No new data to add - all Video IDs already exist in Clean data")
                return 0
            
            logger.info(f"Found {len(new_rows)} NEW rows to append (skipped {len(data_rows) - len(new_rows)} existing)")
            
            # Calculate required rows
            required_rows = current_row_count + len(new_rows)
            required_cols = max(len(row) for row in new_rows) if new_rows else 20
            
            # Get or create 'Clean data' sheet
            try:
                clean_sheet = master.worksheet('Clean data')
                logger.info("Found existing 'Clean data' sheet...")
                
                # Resize if needed
                current_rows = clean_sheet.row_count
                current_cols = clean_sheet.col_count
                
                if current_rows < required_rows or current_cols < required_cols:
                    new_row_count = max(current_rows, required_rows + 1000)
                    new_cols = max(current_cols, required_cols + 5)
                    logger.info(f"Resizing sheet from {current_rows}x{current_cols} to {new_row_count}x{new_cols}...")
                    clean_sheet.resize(rows=new_row_count, cols=new_cols)
                    time.sleep(1)
                
                # NO CLEARING - we're appending!
                
            except gspread.WorksheetNotFound:
                logger.info("Creating 'Clean data' sheet...")
                clean_sheet = master.add_worksheet('Clean data', rows=required_rows + 1000, cols=required_cols + 5)
                current_row_count = 1  # Start after header row
            
            time.sleep(1)
            
            total_rows = len(new_rows)
            # Start writing AFTER existing data
            start_sheet_row = current_row_count + 1 if current_row_count > 1 else 2
            
            logger.info(f"Appending {total_rows} new rows starting at row {start_sheet_row}...")
            
            # Write in batches
            for start_row in range(0, total_rows, BATCH_SIZE):
                end_row = min(start_row + BATCH_SIZE, total_rows)
                batch = new_rows[start_row:end_row]
                batch_num = (start_row // BATCH_SIZE) + 1
                total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
                
                sheet_start_row = start_sheet_row + start_row
                logger.info(f"  Batch {batch_num}/{total_batches}: appending {len(batch)} rows at B{sheet_start_row}")
                
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        # Start at column B
                        cell_range = f'B{sheet_start_row}'
                        clean_sheet.update(values=batch, range_name=cell_range, value_input_option='RAW')
                        break
                    except Exception as e:
                        logger.warning(f"    Write attempt {attempt} failed: {type(e).__name__}: {e}")
                        if attempt == MAX_RETRIES:
                            raise
                        time.sleep(RETRY_DELAY * attempt)
                
                if batch_num % 5 == 0:
                    time.sleep(0.5)
                    logger.info(f"  Progress: {(batch_num/total_batches)*100:.1f}%")
            
            # Add timestamp note
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            clean_sheet.update_note('A1', f'Last updated: {timestamp}')
            
            logger.info(f"✓ Appended {len(new_rows)} new rows!")
            return len(new_rows)
            
        except Exception as e:
            logger.error(f"Failed to write clean data: {e}")
            logger.debug(traceback.format_exc())
            raise
    
    def run(self):
        """Main execution function"""
        start_time = time.time()
        logger.info("=" * 60)
        logger.info("Starting Google Sheets Data Cleaner (Incremental Mode)")
        logger.info("=" * 60)
        
        try:
            # Step 1: Get existing Video IDs from Clean data sheet
            logger.info("Step 1: Reading existing data from Clean data sheet...")
            existing_ids, current_row_count = self.get_existing_video_ids()
            
            # Step 2: Read from Combined Data
            logger.info("Step 2: Reading from Combined Data sheet...")
            data_rows = self.read_combined_data()
            
            if not data_rows:
                logger.warning("No data to clean. Exiting.")
                return
            
            # Step 3: Clean the data (filter valid Play Store links, remove duplicates)
            logger.info("Step 3: Cleaning and filtering data...")
            cleaned_rows = self.clean_data(data_rows)
            
            if not cleaned_rows:
                logger.warning("No valid rows after cleaning. Exiting.")
                return
            
            # Step 4: Append only NEW data to Clean data tab
            logger.info("Step 4: Appending new data to Clean data sheet...")
            new_count = self.write_to_clean_data(cleaned_rows, existing_ids, current_row_count)
            
            # Summary
            elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info(f"✓ SUCCESS!")
            logger.info(f"  Combined Data rows: {len(data_rows)}")
            logger.info(f"  After cleaning: {len(cleaned_rows)} valid rows")
            logger.info(f"  Already in Clean data: {len(existing_ids)}")
            logger.info(f"  NEW rows added: {new_count}")
            logger.info(f"  Time elapsed: {elapsed:.1f} seconds")
            logger.info("=" * 60)
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"✗ FAILED: {e}")
            logger.error(traceback.format_exc())
            logger.error("=" * 60)
            raise

def main():
    """Entry point"""
    # Get credentials from environment
    credentials_json = os.getenv('GOOGLE_CREDENTIALS')
    master_sheet_id = os.getenv('MASTER_SPREADSHEET_ID')
    
    if not credentials_json or not master_sheet_id:
        logger.error("Missing required environment variables:")
        logger.error("  - GOOGLE_CREDENTIALS (service account JSON)")
        logger.error("  - MASTER_SPREADSHEET_ID (your master spreadsheet ID)")
        return
    
    # Run cleaner
    cleaner = DataCleaner(credentials_json, master_sheet_id)
    cleaner.run()

if __name__ == '__main__':
    main()
