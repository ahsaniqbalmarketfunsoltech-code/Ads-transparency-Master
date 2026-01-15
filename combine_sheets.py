#!/usr/bin/env python3
"""
Google Sheets Data Combiner - Optimized for 1M+ rows
Runs on GitHub Actions every hour
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

# Setup logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BATCH_SIZE = 50000  # Rows to write per batch
MAX_RETRIES = 5
RETRY_DELAY = 3  # seconds

class SheetCombiner:
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
    
    def get_sources(self):
        """Read source configurations from 'Sources' tab"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            sources_sheet = master.worksheet('Sources')
            
            # Get all values (skip header row)
            data = sources_sheet.get_all_values()[1:]
            
            sources = []
            for i, row in enumerate(data, start=2):
                if len(row) >= 2 and row[0] and row[1]:
                    # Extract spreadsheet ID from URL
                    url = row[0].strip()
                    tab_name = row[1].strip()
                    
                    # Extract ID from URL
                    if '/spreadsheets/d/' in url:
                        sheet_id = url.split('/spreadsheets/d/')[1].split('/')[0]
                    else:
                        sheet_id = url
                    
                    sources.append({
                        'sheet_id': sheet_id,
                        'tab_name': tab_name,
                        'row_num': i
                    })
            
            logger.info(f"✓ Found {len(sources)} source configurations")
            return sources
        
        except Exception as e:
            logger.error(f"Failed to read sources: {e}")
            raise
    
    def fetch_source_data(self, source):
        """Fetch data from a single source with retry logic"""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"  Fetching from '{source['tab_name']}' (attempt {attempt}/{MAX_RETRIES})")
                
                # Open spreadsheet and worksheet
                spreadsheet = self.client.open_by_key(source['sheet_id'])
                worksheet = spreadsheet.worksheet(source['tab_name'])
                
                # Get all values as DataFrame
                data = worksheet.get_all_values()
                
                if not data:
                    logger.warning(f"  No data found in '{source['tab_name']}'")
                    return None
                
                df = pd.DataFrame(data[1:], columns=data[0])
                logger.info(f"  ✓ Fetched {len(df)} rows from '{source['tab_name']}'")
                
                return df
            
            except PermissionError as e:
                # Permission errors won't be fixed by retrying - skip immediately
                logger.error(f"  ✗ PERMISSION DENIED for spreadsheet: {source['sheet_id']}")
                logger.error(f"    → Please share this spreadsheet with your service account email")
                logger.error(f"    → Skipping this source (retrying won't help)")
                return None
            
            except gspread.exceptions.APIError as e:
                # Check if it's a 403 permission error
                if '403' in str(e) or 'permission' in str(e).lower():
                    logger.error(f"  ✗ PERMISSION DENIED (403) for spreadsheet: {source['sheet_id']}")
                    logger.error(f"    → Please share this spreadsheet with your service account email")
                    logger.error(f"    → Skipping this source (retrying won't help)")
                    return None
                # For other API errors, retry
                logger.warning(f"  Attempt {attempt} failed: APIError: {e}")
                if attempt == MAX_RETRIES:
                    logger.error(f"  ✗ Failed after {MAX_RETRIES} attempts")
                    return None
                time.sleep(RETRY_DELAY * attempt)
            
            except gspread.exceptions.WorksheetNotFound as e:
                logger.error(f"  ✗ Worksheet '{source['tab_name']}' not found in spreadsheet {source['sheet_id']}")
                logger.error(f"    → Skipping this source (check tab name spelling)")
                return None
            
            except gspread.exceptions.SpreadsheetNotFound as e:
                logger.error(f"  ✗ Spreadsheet {source['sheet_id']} not found")
                logger.error(f"    → Check if spreadsheet exists or if ID is correct")
                logger.error(f"    → Skipping this source")
                return None
            
            except Exception as e:
                logger.warning(f"  Attempt {attempt} failed: {type(e).__name__}: {e}")
                logger.debug(f"  Traceback: {traceback.format_exc()}")
                
                if attempt == MAX_RETRIES:
                    logger.error(f"  ✗ Failed after {MAX_RETRIES} attempts")
                    return None
                
                time.sleep(RETRY_DELAY * attempt)
        
        return None
    
    def combine_all_sources(self, sources):
        """Fetch and combine data from all sources"""
        all_dataframes = []
        total_rows = 0
        
        for i, source in enumerate(sources, 1):
            logger.info(f"[{i}/{len(sources)}] Processing '{source['tab_name']}'...")
            
            df = self.fetch_source_data(source)
            
            if df is not None and not df.empty:
                all_dataframes.append(df)
                total_rows += len(df)
                logger.info(f"  Running total: {total_rows} rows")
        
        if not all_dataframes:
            logger.warning("No data collected from any source")
            return None
        
        # Combine all DataFrames
        logger.info("Combining all data...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"✓ Combined {len(combined_df)} total rows")
        
        return combined_df
    
    def write_to_output(self, df):
        """Write combined data to 'Combined Data' tab"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            # Get or create output sheet
            try:
                output_sheet = master.worksheet('Combined Data')
                logger.info("Clearing existing 'Combined Data' sheet...")
                output_sheet.clear()
            except gspread.WorksheetNotFound:
                logger.info("Creating 'Combined Data' sheet...")
                output_sheet = master.add_worksheet('Combined Data', rows=1000, cols=20)
            
            time.sleep(1)  # Wait after clear
            
            # Prepare data (add headers)
            data_to_write = [df.columns.tolist()] + df.values.tolist()
            total_rows = len(data_to_write)
            
            logger.info(f"Writing {total_rows} rows in batches of {BATCH_SIZE}...")
            
            # Write in batches
            for start_row in range(0, total_rows, BATCH_SIZE):
                end_row = min(start_row + BATCH_SIZE, total_rows)
                batch = data_to_write[start_row:end_row]
                batch_num = (start_row // BATCH_SIZE) + 1
                total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
                
                logger.info(f"  Batch {batch_num}/{total_batches}: rows {start_row + 1}-{end_row}")
                
                # Write with retry
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        cell_range = f'A{start_row + 1}'
                        output_sheet.update(cell_range, batch, value_input_option='RAW')
                        break
                    except Exception as e:
                        logger.warning(f"    Write attempt {attempt} failed: {type(e).__name__}: {e}")
                        if attempt == MAX_RETRIES:
                            raise
                        time.sleep(RETRY_DELAY * attempt)
                
                # Pause every 5 batches
                if batch_num % 5 == 0:
                    time.sleep(0.5)
                    logger.info(f"  Progress: {(batch_num/total_batches)*100:.1f}%")
            
            # Format header
            logger.info("Formatting header row...")
            output_sheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.26, 'green': 0.52, 'blue': 0.96}
            })
            
            # Freeze header row
            output_sheet.freeze(rows=1)
            
            # Add timestamp note
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            output_sheet.update_note('A1', f'Last refresh: {timestamp}')
            
            logger.info("✓ Write complete!")
            return True
        
        except Exception as e:
            logger.error(f"Failed to write output: {e}")
            raise
    
    def run(self):
        """Main execution function"""
        start_time = time.time()
        logger.info("=" * 60)
        logger.info("Starting Google Sheets Data Combiner")
        logger.info("=" * 60)
        
        try:
            # Get sources
            sources = self.get_sources()
            
            if not sources:
                logger.warning("No sources configured. Exiting.")
                return
            
            # Fetch and combine
            combined_df = self.combine_all_sources(sources)
            
            if combined_df is None or combined_df.empty:
                logger.warning("No data to write. Exiting.")
                return
            
            # Write to output
            self.write_to_output(combined_df)
            
            # Summary
            elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info(f"✓ SUCCESS!")
            logger.info(f"  Combined {len(combined_df)} rows from {len(sources)} sources")
            logger.info(f"  Time elapsed: {elapsed:.1f} seconds")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"✗ FAILED: {e}")
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
    
    # Run combiner
    combiner = SheetCombiner(credentials_json, master_sheet_id)
    combiner.run()

if __name__ == '__main__':
    main()