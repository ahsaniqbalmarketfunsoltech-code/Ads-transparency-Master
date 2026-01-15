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
    
    def get_youtube_stats_batch(self, video_ids):
        """Fetch YouTube stats for multiple videos in one API call (up to 50)"""
        if not video_ids:
            return {}
        
        # Filter out already cached IDs
        uncached_ids = [vid for vid in video_ids if vid and vid not in self.youtube_cache]
        
        if not uncached_ids:
            # All already cached
            return {vid: self.youtube_cache.get(vid, ('', '')) for vid in video_ids}
        
        # YouTube API allows max 50 IDs per request
        results = {}
        
        for i in range(0, len(uncached_ids), 50):
            batch = uncached_ids[i:i+50]
            
            try:
                params = {
                    'part': 'statistics,snippet',
                    'id': ','.join(batch),  # Comma-separated IDs
                    'key': YOUTUBE_API_KEY
                }
                
                response = requests.get(YOUTUBE_API_URL, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Process each item in response
                    for item in data.get('items', []):
                        vid_id = item.get('id', '')
                        
                        # Get view count
                        view_count = item.get('statistics', {}).get('viewCount', '0')
                        
                        # Get upload date and calculate "time ago"
                        published_at = item.get('snippet', {}).get('publishedAt', '')
                        time_ago = self.calculate_time_ago(published_at)
                        
                        # Cache the result
                        self.youtube_cache[vid_id] = (view_count, time_ago)
                        results[vid_id] = (view_count, time_ago)
                    
                    # Mark missing videos as empty
                    for vid in batch:
                        if vid not in results:
                            self.youtube_cache[vid] = ('', '')
                            results[vid] = ('', '')
                else:
                    logger.warning(f"YouTube API returned status {response.status_code}")
                    for vid in batch:
                        self.youtube_cache[vid] = ('', '')
                        results[vid] = ('', '')
                        
            except Exception as e:
                logger.warning(f"YouTube API batch error: {e}")
                for vid in batch:
                    self.youtube_cache[vid] = ('', '')
                    results[vid] = ('', '')
            
            # Small delay between batches to respect rate limits
            if i + 50 < len(uncached_ids):
                time.sleep(0.2)
        
        # Return results including cached ones
        return {vid: self.youtube_cache.get(vid, ('', '')) for vid in video_ids}
    
    def get_youtube_stats(self, video_id):
        """Fetch YouTube video stats for a single video (uses cache)"""
        if not video_id:
            return '', ''
        
        if video_id in self.youtube_cache:
            return self.youtube_cache[video_id]
        
        # For single requests, use batch method
        results = self.get_youtube_stats_batch([video_id])
        return results.get(video_id, ('', ''))
    
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
    
    def clean_data(self, data_rows, existing_urls=None):
        """Filter rows to keep only those with valid Play Store links, remove duplicates, batch fetch YouTube stats"""
        if not data_rows:
            return []
        
        existing_urls = existing_urls or set()
        
        logger.info(f"Cleaning {len(data_rows)} rows...")
        
        # ============================================
        # PASS 1: Filter valid rows (NO API calls yet)
        # ============================================
        logger.info("  Pass 1: Filtering valid rows...")
        
        valid_rows = []
        removed_invalid = 0
        removed_duplicate = 0
        removed_existing = 0
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
            
            # Get YouTube URL
            youtube_url = row[1] if len(row) > 1 else ''
            
            # Skip if already in Clean data sheet
            if youtube_url and youtube_url in existing_urls:
                removed_existing += 1
                continue
            
            # Mark this Video ID as seen
            if video_id:
                seen_video_ids.add(video_id)
            
            valid_rows.append(row)
        
        logger.info(f"    - Total rows: {len(data_rows)}")
        logger.info(f"    - Invalid App Links removed: {removed_invalid}")
        logger.info(f"    - Duplicate Video IDs removed: {removed_duplicate}")
        logger.info(f"    - Already in Clean data: {removed_existing}")
        logger.info(f"    - Valid NEW rows: {len(valid_rows)}")
        
        if not valid_rows:
            logger.info("  No new valid rows to process. Skipping YouTube API calls.")
            return []
        
        # ============================================
        # PASS 2: Collect all video IDs for batch API
        # ============================================
        logger.info("  Pass 2: Collecting video IDs for batch API call...")
        
        video_id_map = {}  # Map video_id -> list of row indices
        for idx, row in enumerate(valid_rows):
            youtube_url = row[1] if len(row) > 1 else ''
            video_id = row[4] if len(row) > 4 else ''
            
            # Extract video ID from URL or use existing
            yt_video_id = self.extract_video_id(youtube_url) or video_id
            if yt_video_id:
                video_id_map[yt_video_id] = video_id_map.get(yt_video_id, [])
                video_id_map[yt_video_id].append(idx)
        
        unique_video_ids = list(video_id_map.keys())
        logger.info(f"    - Unique video IDs to fetch: {len(unique_video_ids)}")
        
        # ============================================
        # PASS 3: Batch fetch YouTube stats (50 per API call)
        # ============================================
        if unique_video_ids:
            api_calls_needed = (len(unique_video_ids) + 49) // 50
            logger.info(f"  Pass 3: Fetching YouTube stats ({api_calls_needed} API calls for {len(unique_video_ids)} videos)...")
            self.get_youtube_stats_batch(unique_video_ids)
            logger.info(f"    - YouTube stats cached for {len(unique_video_ids)} videos")
        
        # ============================================
        # PASS 4: Build output rows with cached stats
        # ============================================
        logger.info("  Pass 4: Building output rows...")
        
        cleaned_rows = []
        for row in valid_rows:
            # Source indices: 0=Advertiser, 1=Ads URL (YouTube link), 2=App Link, 3=App Name, 4=Video ID
            youtube_url = row[1] if len(row) > 1 else ''
            app_link_val = row[2] if len(row) > 2 else ''
            app_name = row[3] if len(row) > 3 else ''
            advertiser = row[0] if len(row) > 0 else ''
            video_id = row[4] if len(row) > 4 else ''
            
            # Get cached YouTube stats
            yt_video_id = self.extract_video_id(youtube_url) or video_id
            view_count, time_ago = self.youtube_cache.get(yt_video_id, ('', ''))
            
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
        logger.info(f"  - Output rows: {len(cleaned_rows)}")
        logger.info(f"  - API calls made: {(len(unique_video_ids) + 49) // 50}")
        logger.info(f"  - Quota saved: {len(unique_video_ids) - ((len(unique_video_ids) + 49) // 50)} units")
        
        return cleaned_rows
    
    def get_existing_video_ids(self):
        """Read existing data from Clean data sheet - URLs and rows missing views"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            try:
                clean_sheet = master.worksheet('Clean data')
                # Get all values
                all_data = clean_sheet.get_all_values()
                
                # Skip header row
                # Column D (index 3): Full Youtube links
                # Column H (index 7): Yt Views
                existing_urls = set()
                rows_missing_views = []  # List of (row_number, youtube_url) that need stats
                
                for idx, row in enumerate(all_data[1:], start=2):  # Start at row 2 (skip header)
                    youtube_url = row[3] if len(row) > 3 else ''
                    yt_views = row[7] if len(row) > 7 else ''
                    
                    if youtube_url:
                        existing_urls.add(youtube_url)
                        
                        # Check if this row is missing views
                        if not yt_views or yt_views.strip() == '':
                            rows_missing_views.append((idx, youtube_url))
                
                logger.info(f"✓ Found {len(existing_urls)} existing YouTube URLs in Clean data")
                logger.info(f"  - Rows missing views: {len(rows_missing_views)}")
                
                return existing_urls, len(all_data), rows_missing_views
                
            except gspread.WorksheetNotFound:
                logger.info("Clean data sheet not found, will create new one")
                return set(), 1, []
                
        except Exception as e:
            logger.error(f"Failed to read existing data: {e}")
            logger.debug(traceback.format_exc())
            return set(), 1, []
    
    def update_missing_views(self, rows_missing_views):
        """Fetch YouTube stats for existing rows that are missing views and update them"""
        if not rows_missing_views:
            logger.info("No rows missing views to update.")
            return 0
        
        logger.info(f"Updating {len(rows_missing_views)} rows with missing views...")
        
        # Extract video IDs from URLs
        video_ids_to_fetch = []
        row_video_map = {}  # video_id -> list of row numbers
        
        for row_num, youtube_url in rows_missing_views:
            video_id = self.extract_video_id(youtube_url)
            if video_id:
                video_ids_to_fetch.append(video_id)
                if video_id not in row_video_map:
                    row_video_map[video_id] = []
                row_video_map[video_id].append(row_num)
        
        if not video_ids_to_fetch:
            logger.info("No valid video IDs found in rows missing views.")
            return 0
        
        # Batch fetch YouTube stats
        unique_ids = list(set(video_ids_to_fetch))
        api_calls_needed = (len(unique_ids) + 49) // 50
        logger.info(f"  Fetching stats for {len(unique_ids)} unique videos ({api_calls_needed} API calls)...")
        
        self.get_youtube_stats_batch(unique_ids)
        
        # Update the cells in the sheet
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            clean_sheet = master.worksheet('Clean data')
            
            # Prepare batch updates
            updates = []
            for row_num, youtube_url in rows_missing_views:
                video_id = self.extract_video_id(youtube_url)
                if video_id and video_id in self.youtube_cache:
                    view_count, time_ago = self.youtube_cache[video_id]
                    # Column H (views) = column 8, Column I (time) = column 9
                    updates.append({
                        'range': f'H{row_num}:I{row_num}',
                        'values': [[view_count, time_ago]]
                    })
            
            if updates:
                # Batch update in chunks of 100
                for i in range(0, len(updates), 100):
                    batch = updates[i:i+100]
                    clean_sheet.batch_update(batch, value_input_option='RAW')
                    logger.info(f"  Updated {min(i+100, len(updates))}/{len(updates)} rows...")
                    time.sleep(0.5)
                
                logger.info(f"✓ Updated {len(updates)} rows with YouTube stats")
                return len(updates)
            
        except Exception as e:
            logger.error(f"Failed to update missing views: {e}")
            logger.debug(traceback.format_exc())
        
        return 0
    
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
            # Step 1: Get existing data from Clean data sheet
            logger.info("Step 1: Reading existing data from Clean data sheet...")
            existing_urls, current_row_count, rows_missing_views = self.get_existing_video_ids()
            
            # Step 2: Update existing rows that are missing views
            updated_count = 0
            if rows_missing_views:
                logger.info("Step 2: Updating existing rows with missing views...")
                updated_count = self.update_missing_views(rows_missing_views)
            else:
                logger.info("Step 2: No existing rows missing views - skipping update")
            
            # Step 3: Read from Combined Data
            logger.info("Step 3: Reading from Combined Data sheet...")
            data_rows = self.read_combined_data()
            
            if not data_rows:
                logger.warning("No data in Combined Data. Exiting.")
                elapsed = time.time() - start_time
                logger.info(f"✓ Done! Updated {updated_count} existing rows. Time: {elapsed:.1f}s")
                return
            
            # Step 4: Clean the data, fetch YouTube stats for NEW rows only
            logger.info("Step 4: Cleaning data and fetching YouTube stats for new rows...")
            cleaned_rows = self.clean_data(data_rows, existing_urls)
            
            new_count = 0
            if cleaned_rows:
                # Step 5: Append only NEW data to Clean data tab
                logger.info("Step 5: Appending new data to Clean data sheet...")
                new_count = self.write_to_clean_data(cleaned_rows, existing_urls, current_row_count)
            else:
                logger.info("Step 5: No new rows to add - all data already exists")
            
            # Summary
            elapsed = time.time() - start_time
            api_calls = ((len(rows_missing_views) + 49) // 50) + ((new_count + 49) // 50) if new_count > 0 else (len(rows_missing_views) + 49) // 50
            
            logger.info("=" * 60)
            logger.info(f"✓ SUCCESS!")
            logger.info(f"  Combined Data rows: {len(data_rows)}")
            logger.info(f"  Already in Clean data: {len(existing_urls)}")
            logger.info(f"  Existing rows updated (missing views): {updated_count}")
            logger.info(f"  NEW rows added: {new_count}")
            logger.info(f"  Time elapsed: {elapsed:.1f} seconds")
            logger.info(f"  YouTube API quota used: ~{api_calls} units")
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
