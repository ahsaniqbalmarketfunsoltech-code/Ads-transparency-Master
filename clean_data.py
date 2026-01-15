#!/usr/bin/env python3
"""
Google Sheets Data Cleaner with YouTube API Integration
Reads from 'Combined Data' tab, filters valid Play Store links, 
fetches YouTube stats, writes to 'Clean data' tab
Runs on GitHub Actions every hour (after combine-sheets workflow)
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
import pandas as pd
import numpy as np
import requests

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

# YouTube API Configuration
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY', 'AIzaSyCov_GZu8554LbvjERy2VCbRWnhUjIToZA')
YOUTUBE_API_URL = 'https://www.googleapis.com/youtube/v3/videos'

# Column mapping from Combined Data (0-based indices)
# Source columns in Combined Data:
#   0: Advertiser Name
#   1: Ads URL (Full YouTube link)
#   2: App Link ← Filter by this (must be valid Play Store link)
#   3: App Name
#   4: Video ID

# App Link column index in SOURCE data (for filtering)
APP_LINK_COLUMN_INDEX = 2

class DataCleaner:
    def __init__(self, credentials_json, master_sheet_id):
        """Initialize with service account credentials"""
        self.master_sheet_id = master_sheet_id
        self.youtube_cache = {}  # Cache for YouTube stats
        
        # Authenticate
        creds_dict = json.loads(credentials_json)
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        self.client = gspread.authorize(creds)
        
        logger.info("✓ Authenticated with Google Sheets API")
    
    def extract_video_id(self, url):
        """Extract YouTube video ID from URL"""
        if not url:
            return None
        
        # Handle various YouTube URL formats
        patterns = [
            r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
            r'youtu\.be/([a-zA-Z0-9_-]{11})',
            r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
            r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return None
    
    def get_youtube_stats(self, video_id):
        """Fetch YouTube video stats using API"""
        if not video_id:
            return None, None
        
        # Check cache first
        if video_id in self.youtube_cache:
            return self.youtube_cache[video_id]
        
        try:
            params = {
                'part': 'statistics,snippet',
                'id': video_id,
                'key': YOUTUBE_API_KEY
            }
            
            response = requests.get(YOUTUBE_API_URL, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('items') and len(data['items']) > 0:
                    item = data['items'][0]
                    
                    # Get view count
                    view_count = item.get('statistics', {}).get('viewCount', '0')
                    
                    # Get upload date and calculate "time ago"
                    published_at = item.get('snippet', {}).get('publishedAt', '')
                    time_ago = self.calculate_time_ago(published_at)
                    
                    # Cache the result
                    self.youtube_cache[video_id] = (view_count, time_ago)
                    return view_count, time_ago
            
            # API error or video not found
            self.youtube_cache[video_id] = ('', '')
            return '', ''
            
        except Exception as e:
            logger.debug(f"YouTube API error for {video_id}: {e}")
            self.youtube_cache[video_id] = ('', '')
            return '', ''
    
    def calculate_time_ago(self, published_at):
        """Calculate how long ago a video was uploaded"""
        if not published_at:
            return ''
        
        try:
            # Parse ISO 8601 date
            upload_date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            
            diff = now - upload_date
            
            days = diff.days
            if days < 1:
                hours = diff.seconds // 3600
                return f"{hours} hours ago" if hours != 1 else "1 hour ago"
            elif days < 7:
                return f"{days} days ago" if days != 1 else "1 day ago"
            elif days < 30:
                weeks = days // 7
                return f"{weeks} weeks ago" if weeks != 1 else "1 week ago"
            elif days < 365:
                months = days // 30
                return f"{months} months ago" if months != 1 else "1 month ago"
            else:
                years = days // 365
                return f"{years} years ago" if years != 1 else "1 year ago"
                
        except Exception as e:
            logger.debug(f"Error parsing date {published_at}: {e}")
            return ''
    
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
        """Filter rows to keep only those with valid Play Store links, remove duplicate Video IDs, fetch YouTube stats"""
        if not data_rows:
            return []
        
        logger.info(f"Cleaning {len(data_rows)} rows...")
        
        # Output column mapping (starting from column D):
        # Clean data headers:
        #   D: Full Youtube links  → output position 0
        #   E: App Link            → output position 1
        #   F: App Name            → output position 2
        #   G: Advertiser Name     → output position 3
        #   H: Yt Views            → output position 4
        #   I: Upload Time         → output position 5
        
        cleaned_rows = []
        removed_invalid = 0
        removed_duplicate = 0
        seen_video_ids = set()
        youtube_api_calls = 0
        
        total = len(data_rows)
        for idx, row in enumerate(data_rows):
            # Progress logging every 10000 rows
            if idx > 0 and idx % 10000 == 0:
                logger.info(f"  Processing row {idx}/{total}...")
            
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
            
            # Get source data
            # Source indices: 0=Advertiser, 1=Ads URL (YouTube link), 2=App Link, 3=App Name, 4=Video ID
            youtube_url = row[1] if len(row) > 1 else ''
            app_link_val = row[2] if len(row) > 2 else ''
            app_name = row[3] if len(row) > 3 else ''
            advertiser = row[0] if len(row) > 0 else ''
            
            # Fetch YouTube stats (view count and upload time)
            # Try to get video ID from URL or use the existing one
            yt_video_id = self.extract_video_id(youtube_url) or video_id
            view_count, time_ago = self.get_youtube_stats(yt_video_id)
            youtube_api_calls += 1
            
            # Rate limit: small delay every 100 API calls
            if youtube_api_calls % 100 == 0:
                time.sleep(0.5)
            
            # Output row for columns D through I (6 columns):
            # D=Full Youtube, E=App Link, F=App Name, G=Advertiser, H=Views, I=Upload Time
            output_row = [
                youtube_url,   # D: Full Youtube links
                app_link_val,  # E: App Link
                app_name,      # F: App Name
                advertiser,    # G: Advertiser Name
                view_count,    # H: Yt Views
                time_ago       # I: Upload Time
            ]
            
            cleaned_rows.append(output_row)
        
        logger.info(f"✓ Cleaning complete:")
        logger.info(f"  - Kept: {len(cleaned_rows)} unique valid rows")
        logger.info(f"  - Removed invalid App Links: {removed_invalid}")
        logger.info(f"  - Removed duplicate Video IDs: {removed_duplicate}")
        logger.info(f"  - YouTube API calls: {youtube_api_calls}")
        logger.info(f"  - Output: D=Youtube, E=App Link, F=App Name, G=Advertiser, H=Views, I=Time")
        
        return cleaned_rows
    
    def get_existing_video_ids(self):
        """Read existing YouTube URLs from Clean data sheet to avoid duplicates"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            try:
                clean_sheet = master.worksheet('Clean data')
                # Get all values
                all_data = clean_sheet.get_all_values()
                
                # Skip header row, get YouTube URLs from column D (index 3, since we read from A)
                # Column D contains Full Youtube links
                existing_urls = set()
                for row in all_data[1:]:  # Skip header
                    if len(row) > 3 and row[3]:  # Column D is index 3
                        existing_urls.add(row[3])
                
                logger.info(f"✓ Found {len(existing_urls)} existing YouTube URLs in Clean data")
                return existing_urls, len(all_data)  # Return URLs and current row count
                
            except gspread.WorksheetNotFound:
                logger.info("Clean data sheet not found, will create new one")
                return set(), 1  # No existing URLs, start after header row
                
        except Exception as e:
            logger.error(f"Failed to read existing data: {e}")
            logger.debug(traceback.format_exc())
            return set(), 1
    
    def write_to_clean_data(self, data_rows, existing_urls, current_row_count):
        """Append new cleaned data to 'Clean data' tab (incremental, no clearing)"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            # Filter out rows that already exist in Clean data (by YouTube URL)
            new_rows = []
            for row in data_rows:
                youtube_url = row[0]  # YouTube URL is first column in output
                if youtube_url and youtube_url not in existing_urls:
                    new_rows.append(row)
            
            if not new_rows:
                logger.info("✓ No new data to add - all YouTube URLs already exist in Clean data")
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
            
            logger.info(f"Appending {total_rows} new rows starting at D{start_sheet_row}...")
            
            # Write in batches
            for start_row in range(0, total_rows, BATCH_SIZE):
                end_row = min(start_row + BATCH_SIZE, total_rows)
                batch = new_rows[start_row:end_row]
                batch_num = (start_row // BATCH_SIZE) + 1
                total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
                
                sheet_start_row = start_sheet_row + start_row
                logger.info(f"  Batch {batch_num}/{total_batches}: appending {len(batch)} rows at D{sheet_start_row}")
                
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        # Start at column D (not B)
                        cell_range = f'D{sheet_start_row}'
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
            clean_sheet.update_note('D1', f'Last updated: {timestamp}')
            
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
        logger.info("Starting Google Sheets Data Cleaner with YouTube API")
        logger.info("=" * 60)
        
        try:
            # Step 1: Get existing YouTube URLs from Clean data sheet
            logger.info("Step 1: Reading existing data from Clean data sheet...")
            existing_urls, current_row_count = self.get_existing_video_ids()
            
            # Step 2: Read from Combined Data
            logger.info("Step 2: Reading from Combined Data sheet...")
            data_rows = self.read_combined_data()
            
            if not data_rows:
                logger.warning("No data to clean. Exiting.")
                return
            
            # Step 3: Clean the data, fetch YouTube stats
            logger.info("Step 3: Cleaning data and fetching YouTube stats...")
            logger.info(f"  (This may take a while - fetching stats for each video)")
            cleaned_rows = self.clean_data(data_rows)
            
            if not cleaned_rows:
                logger.warning("No valid rows after cleaning. Exiting.")
                return
            
            # Step 4: Append only NEW data to Clean data tab
            logger.info("Step 4: Appending new data to Clean data sheet...")
            new_count = self.write_to_clean_data(cleaned_rows, existing_urls, current_row_count)
            
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
