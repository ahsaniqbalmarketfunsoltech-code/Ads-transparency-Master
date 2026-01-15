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
#   0: Video ID
#   1: Base 64 (SKIP)
#   2: Full Youtube links (SKIP)
#   3: App Link
#   4: App Name
#   5: Advertiser Name

# Columns to KEEP (indices in source data) - in order they should appear in output
COLUMNS_TO_KEEP = [0, 3, 4, 5]  # Video ID, App Link, App Name, Advertiser Name

# App Link column index in SOURCE data (for filtering)
APP_LINK_COLUMN_INDEX = 3

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
        """Filter rows to keep only those with valid Play Store links, and select specific columns"""
        if not data_rows:
            return []
        
        logger.info(f"Cleaning {len(data_rows)} rows...")
        logger.info(f"  Keeping columns at indices: {COLUMNS_TO_KEEP}")
        
        cleaned_rows = []
        removed_count = 0
        
        for row in data_rows:
            # Get App Link value for filtering
            app_link = row[APP_LINK_COLUMN_INDEX] if len(row) > APP_LINK_COLUMN_INDEX else ''
            
            if self.is_valid_playstore_link(app_link):
                # Extract only the columns we want to keep
                filtered_row = []
                for col_idx in COLUMNS_TO_KEEP:
                    if col_idx < len(row):
                        filtered_row.append(row[col_idx])
                    else:
                        filtered_row.append('')  # Empty if column doesn't exist
                
                cleaned_rows.append(filtered_row)
            else:
                removed_count += 1
        
        logger.info(f"✓ Cleaning complete:")
        logger.info(f"  - Kept: {len(cleaned_rows)} rows with valid Play Store links")
        logger.info(f"  - Removed: {removed_count} rows with invalid/missing App Links")
        logger.info(f"  - Output columns: Video ID, App Link, App Name, Advertiser Name")
        
        return cleaned_rows
    
    def write_to_clean_data(self, data_rows):
        """Write cleaned data to 'Clean data' tab"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            # Calculate required rows
            required_rows = len(data_rows) + 1  # +1 for header row
            required_cols = max(len(row) for row in data_rows) if data_rows else 20
            
            # Get or create 'Clean data' sheet
            try:
                clean_sheet = master.worksheet('Clean data')
                logger.info("Found existing 'Clean data' sheet...")
                
                # Resize if needed
                current_rows = clean_sheet.row_count
                current_cols = clean_sheet.col_count
                
                if current_rows < required_rows or current_cols < required_cols:
                    new_rows = max(current_rows, required_rows + 1000)
                    new_cols = max(current_cols, required_cols + 5)
                    logger.info(f"Resizing sheet from {current_rows}x{current_cols} to {new_rows}x{new_cols}...")
                    clean_sheet.resize(rows=new_rows, cols=new_cols)
                    time.sleep(1)
                
                # Clear only data rows (row 2 onwards), preserve header in row 1
                logger.info("Clearing data rows (preserving row 1 header)...")
                clean_sheet.batch_clear([f'A2:Z{clean_sheet.row_count}'])
                
            except gspread.WorksheetNotFound:
                logger.info("Creating 'Clean data' sheet...")
                clean_sheet = master.add_worksheet('Clean data', rows=required_rows + 1000, cols=required_cols + 5)
            
            time.sleep(1)
            
            if not data_rows:
                logger.warning("No data to write after cleaning")
                return
            
            total_rows = len(data_rows)
            logger.info(f"Writing {total_rows} clean rows in batches of {BATCH_SIZE} (starting at row 2)...")
            
            # Write in batches - START AT ROW 2 to preserve header
            for start_row in range(0, total_rows, BATCH_SIZE):
                end_row = min(start_row + BATCH_SIZE, total_rows)
                batch = data_rows[start_row:end_row]
                batch_num = (start_row // BATCH_SIZE) + 1
                total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
                
                sheet_start_row = start_row + 2  # Row 1 is header
                logger.info(f"  Batch {batch_num}/{total_batches}: rows {start_row + 1}-{end_row} → sheet rows {sheet_start_row}-{sheet_start_row + len(batch) - 1}")
                
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        cell_range = f'A{sheet_start_row}'
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
            clean_sheet.update_note('A1', f'Last cleaned: {timestamp}')
            
            logger.info("✓ Write complete!")
            
        except Exception as e:
            logger.error(f"Failed to write clean data: {e}")
            logger.debug(traceback.format_exc())
            raise
    
    def run(self):
        """Main execution function"""
        start_time = time.time()
        logger.info("=" * 60)
        logger.info("Starting Google Sheets Data Cleaner")
        logger.info("=" * 60)
        
        try:
            # Read from Combined Data
            data_rows = self.read_combined_data()
            
            if not data_rows:
                logger.warning("No data to clean. Exiting.")
                return
            
            # Clean the data
            cleaned_rows = self.clean_data(data_rows)
            
            if not cleaned_rows:
                logger.warning("No valid rows after cleaning. Exiting.")
                return
            
            # Write to Clean data tab
            self.write_to_clean_data(cleaned_rows)
            
            # Summary
            elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info(f"✓ SUCCESS!")
            logger.info(f"  Original rows: {len(data_rows)}")
            logger.info(f"  Clean rows: {len(cleaned_rows)}")
            logger.info(f"  Removed: {len(data_rows) - len(cleaned_rows)} invalid rows")
            logger.info(f"  Time elapsed: {elapsed:.1f} seconds")
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
