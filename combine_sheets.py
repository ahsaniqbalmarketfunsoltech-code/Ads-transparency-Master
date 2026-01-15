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
                
                # Get all values (skip header row)
                data = worksheet.get_all_values()
                
                if not data:
                    logger.warning(f"  No data found in '{source['tab_name']}'")
                    return None
                
                # Skip header row (index 0), just take data rows
                data_rows = data[1:] if len(data) > 1 else []
                
                if not data_rows:
                    logger.warning(f"  No data rows found in '{source['tab_name']}' (only header)")
                    return None
                
                # Create DataFrame without using headers as column names
                df = pd.DataFrame(data_rows)
                logger.info(f"  ✓ Fetched {len(df)} data rows from '{source['tab_name']}' (header skipped)")
                
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
    
    def _fix_duplicate_columns(self, df):
        """Rename duplicate columns to make them unique"""
        cols = df.columns.tolist()
        seen = {}
        new_cols = []
        
        for col in cols:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 1
                new_cols.append(col)
        
        if new_cols != cols:
            logger.debug(f"    Fixed {len(cols) - len(set(cols))} duplicate column(s)")
            df.columns = new_cols
        
        return df
    
    def combine_all_sources(self, sources):
        """Fetch and combine data from all sources"""
        all_dataframes = []
        total_rows = 0
        
        for i, source in enumerate(sources, 1):
            logger.info(f"[{i}/{len(sources)}] Processing '{source['tab_name']}'...")
            
            df = self.fetch_source_data(source)
            
            if df is not None and not df.empty:
                # Fix any duplicate column names in this DataFrame
                df = self._fix_duplicate_columns(df)
                all_dataframes.append(df)
                total_rows += len(df)
                logger.info(f"  Running total: {total_rows} rows")
        
        if not all_dataframes:
            logger.warning("No data collected from any source")
            return None
        
        # Combine all DataFrames
        logger.info("Combining all data...")
        try:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
        except Exception as e:
            logger.warning(f"Standard concat failed: {e}")
            logger.info("Attempting alternative concat method...")
            # Alternative: convert all to same columns structure
            all_columns = set()
            for df in all_dataframes:
                all_columns.update(df.columns.tolist())
            all_columns = sorted(list(all_columns))
            
            # Reindex all dataframes to have same columns
            aligned_dfs = []
            for df in all_dataframes:
                aligned_df = df.reindex(columns=all_columns, fill_value='')
                aligned_dfs.append(aligned_df)
            
            combined_df = pd.concat(aligned_dfs, ignore_index=True)
        
        logger.info(f"✓ Combined {len(combined_df)} total rows")
        
        return combined_df
    
    def write_to_output(self, df):
        """Write combined data to 'Combined Data' tab"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            # Calculate required rows (data rows + 1 for header)
            required_rows = len(df) + 1  # +1 for user's manual header row
            required_cols = len(df.columns) if len(df.columns) > 0 else 20
            
            # Get or create output sheet
            try:
                output_sheet = master.worksheet('Combined Data')
                logger.info("Found existing 'Combined Data' sheet...")
                
                # Get current sheet size
                current_rows = output_sheet.row_count
                current_cols = output_sheet.col_count
                
                # Resize if needed BEFORE clearing/writing
                if current_rows < required_rows or current_cols < required_cols:
                    new_rows = max(current_rows, required_rows + 1000)  # Add buffer
                    new_cols = max(current_cols, required_cols + 5)
                    logger.info(f"Resizing sheet from {current_rows}x{current_cols} to {new_rows}x{new_cols}...")
                    output_sheet.resize(rows=new_rows, cols=new_cols)
                    time.sleep(1)
                
                # Clear only data rows (row 2 onwards), preserve header in row 1
                logger.info("Clearing data rows (preserving row 1 for your header)...")
                # Clear from A2 to the end
                output_sheet.batch_clear([f'A2:Z{output_sheet.row_count}'])
                
            except gspread.WorksheetNotFound:
                logger.info("Creating 'Combined Data' sheet...")
                # Create with enough rows from the start
                output_sheet = master.add_worksheet('Combined Data', rows=required_rows + 1000, cols=required_cols + 5)
            
            time.sleep(1)  # Wait after operations
            
            # Clean DataFrame - replace NaN, inf, -inf with empty strings
            logger.info("Cleaning data for JSON compatibility...")
            import numpy as np
            df = df.fillna('')  # Replace NaN with empty string
            df = df.replace([np.inf, -np.inf], '')  # Replace inf values
            
            # Convert all values to strings to avoid JSON issues
            df = df.astype(str)
            df = df.replace('nan', '')
            df = df.replace('None', '')
            
            # Prepare data (NO headers - user will add manually in row 1)
            data_to_write = df.values.tolist()
            total_rows = len(data_to_write)
            
            logger.info(f"Writing {total_rows} data rows in batches of {BATCH_SIZE} (starting at row 2)...")
            
            # Write in batches - START AT ROW 2 to preserve user's header in row 1
            for start_row in range(0, total_rows, BATCH_SIZE):
                end_row = min(start_row + BATCH_SIZE, total_rows)
                batch = data_to_write[start_row:end_row]
                batch_num = (start_row // BATCH_SIZE) + 1
                total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
                
                # Sheet row = data row + 2 (row 1 is header, data starts at row 2)
                sheet_start_row = start_row + 2
                logger.info(f"  Batch {batch_num}/{total_batches}: data rows {start_row + 1}-{end_row} → sheet rows {sheet_start_row}-{sheet_start_row + len(batch) - 1}")
                
                # Write with retry (using named arguments for gspread 6.x)
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        cell_range = f'A{sheet_start_row}'
                        output_sheet.update(values=batch, range_name=cell_range, value_input_option='RAW')
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