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
#   0 (A): Advertiser Name
#   1 (B): Ads URL
#   2 (C): App Link ← Filter by this (must be valid Play Store link)
#   3 (D): App Name
#   4 (E): Video ID ← For duplicate detection
#   5 (F): Full Youtube links

# Column indices
APP_LINK_COLUMN_INDEX = 2   # Column C
VIDEO_ID_COLUMN_INDEX = 4   # Column E
YOUTUBE_URL_COLUMN_INDEX = 5  # Column F

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
    
    def hex_to_base64(self, hex_string):
        """
        Convert a hex string (Video ID from Column B) to YouTube Base64 code.
        This replicates the Google Sheets formula logic.
        """
        if not hex_string or not isinstance(hex_string, str):
            return ''
        
        # Clean the hex string
        hex_string = hex_string.strip().lower()
        
        # Validate hex string (must be even length and valid hex chars)
        if len(hex_string) % 2 != 0:
            return ''
        if not all(c in '0123456789abcdef' for c in hex_string):
            return ''
        
        try:
            # YouTube's URL-safe Base64 alphabet
            alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            
            # Convert hex to bytes
            num_bytes = len(hex_string) // 2
            binary_str = ''
            for i in range(num_bytes):
                byte_hex = hex_string[i*2:(i*2)+2]
                byte_val = int(byte_hex, 16)
                binary_str += format(byte_val, '08b')  # 8-bit binary
            
            # Pad binary string to make length divisible by 6
            bits = len(binary_str)
            groups = (bits + 5) // 6  # Ceiling division
            padded = binary_str + '0' * (groups * 6 - bits)
            
            # Convert 6-bit groups to base64 characters
            base64_result = ''
            for g in range(groups):
                six_bits = padded[g*6:(g*6)+6]
                index = int(six_bits, 2)
                base64_result += alphabet[index]
            
            return base64_result
        except Exception as e:
            logger.debug(f"Failed to convert hex '{hex_string}' to base64: {e}")
            return ''
    
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
            
            # Get YouTube URL and extract video ID
            youtube_url = row[1] if len(row) > 1 else ''
            yt_video_id = self.extract_video_id(youtube_url) or video_id
            
            # Skip if we've already seen this Video ID in this batch
            if yt_video_id and yt_video_id in seen_video_ids:
                removed_duplicate += 1
                continue
            
            # Skip if already in Clean data sheet (compare by video ID)
            if yt_video_id and yt_video_id in existing_urls:
                removed_existing += 1
                continue
            
            # Mark this Video ID as seen
            if yt_video_id:
                seen_video_ids.add(yt_video_id)
            
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
    
    def clean_data_no_youtube(self, data_rows, existing_video_ids):
        """Filter rows to keep only valid NEW rows (NO YouTube API calls)"""
        if not data_rows:
            return []
        
        existing_video_ids = existing_video_ids or set()
        
        logger.info(f"Filtering {len(data_rows)} rows...")
        logger.info(f"  Already in Clean data: {len(existing_video_ids)} Video IDs")
        
        valid_rows = []
        removed_invalid = 0
        removed_duplicate = 0
        removed_existing = 0
        seen_video_ids = set()
        
        for row in data_rows:
            # Filter 1: Valid Play Store link in App Link column (C, index 2)
            app_link = row[APP_LINK_COLUMN_INDEX] if len(row) > APP_LINK_COLUMN_INDEX else ''
            
            if not self.is_valid_playstore_link(app_link):
                removed_invalid += 1
                continue
            
            # Get Video ID from column E (index 4) - use directly for duplicate check
            video_id = row[VIDEO_ID_COLUMN_INDEX] if len(row) > VIDEO_ID_COLUMN_INDEX else ''
            
            if not video_id:
                removed_invalid += 1
                continue
            
            # Filter 2: Remove duplicates (already seen in this batch)
            if video_id in seen_video_ids:
                removed_duplicate += 1
                continue
            
            # Filter 3: Skip if already in Clean data
            if video_id in existing_video_ids:
                removed_existing += 1
                continue
            
            # Mark this Video ID as seen
            seen_video_ids.add(video_id)
            
            # Build output row (Video ID, App Link, App Name, Advertiser Name)
            app_name = row[3] if len(row) > 3 else ''
            advertiser = row[0] if len(row) > 0 else ''
            
            output_row = [
                video_id,      # For Column B
                app_link,      # For Column E
                app_name,      # For Column F
                advertiser     # For Column G
            ]
            
            valid_rows.append(output_row)
        
        logger.info(f"  - Total rows: {len(data_rows)}")
        logger.info(f"  - Invalid (no valid App Link or Video ID): {removed_invalid}")
        logger.info(f"  - Duplicate Video IDs: {removed_duplicate}")
        logger.info(f"  - Already in Clean data: {removed_existing}")
        logger.info(f"  - NEW rows to add: {len(valid_rows)}")
        
        return valid_rows
    
    def write_new_rows(self, data_rows, current_row_count):
        """Write new rows to Clean data sheet (touching ONLY columns B and E-G)"""
        if not data_rows:
            return 0
        
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            clean_sheet = master.worksheet('Clean data')
            
            # Calculate required size
            required_rows = current_row_count + len(data_rows)
            
            # Resize if needed
            if clean_sheet.row_count < required_rows:
                clean_sheet.resize(rows=required_rows + 1000)
                time.sleep(1)
            
            # Start writing after existing data
            start_row = current_row_count + 1
            
            logger.info(f"  Appending {len(data_rows)} new rows (touching only B and E:G)...")
            
            # Prepare batch updates for efficiency
            total_rows = len(data_rows)
            # We'll process in chunks to avoid extremely large payload sizes, 
            # though GSheets API handles quite a bit.
            for start_idx in range(0, total_rows, BATCH_SIZE):
                end_idx = min(start_idx + BATCH_SIZE, total_rows)
                chunk = data_rows[start_idx:end_idx]
                chunk_start_row = start_row + start_idx
                chunk_end_row = chunk_start_row + len(chunk) - 1
                
                updates = []
                
                # Update Column B: Video ID
                b_values = [[row[0]] for row in chunk]
                updates.append({
                    'range': f'B{chunk_start_row}:B{chunk_end_row}',
                    'values': b_values
                })
                
                # Update Column E:G : App Link, App Name, Advertiser Name
                efg_values = [row[1:4] for row in chunk]
                updates.append({
                    'range': f'E{chunk_start_row}:G{chunk_end_row}',
                    'values': efg_values
                })
                
                # Execute batch update for this chunk
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        clean_sheet.batch_update(updates, value_input_option='RAW')
                        break
                    except Exception as e:
                        logger.warning(f"Batch update attempt {attempt} failed: {e}")
                        if attempt == MAX_RETRIES:
                            raise
                        time.sleep(RETRY_DELAY * attempt)
            
            logger.info(f"  ✓ Appended {len(data_rows)} new rows successfully")
            return len(data_rows)
            
        except Exception as e:
            logger.error(f"Failed to write new rows: {e}")
            logger.debug(traceback.format_exc())
            raise
    
    def convert_hex_to_youtube_urls(self):
        """
        Step 3: Convert Hex Video IDs (Column B) to Base64 (Column C) and YouTube URLs (Column D).
        Only processes rows where Column C or Column D are empty but Column B has a value.
        """
        logger.info("Step 3: Converting Hex IDs to Base64 and YouTube URLs...")
        
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            clean_sheet = master.worksheet('Clean data')
            
            # Get all values
            all_data = clean_sheet.get_all_values()
            
            if len(all_data) <= 1:
                logger.info("  No data rows to process.")
                return 0
            
            # Find rows that need conversion (have B but missing C or D)
            rows_to_update = []
            
            for idx, row in enumerate(all_data[1:], start=2):  # Skip header
                video_id_hex = row[1] if len(row) > 1 else ''  # Column B
                base64_code = row[2] if len(row) > 2 else ''   # Column C
                youtube_url = row[3] if len(row) > 3 else ''   # Column D
                
                # Check if we need to convert
                if video_id_hex and video_id_hex.strip():
                    needs_base64 = not base64_code or not base64_code.strip()
                    needs_url = not youtube_url or not youtube_url.strip()
                    
                    if needs_base64 or needs_url:
                        # Convert hex to base64
                        converted_base64 = self.hex_to_base64(video_id_hex)
                        
                        if converted_base64:
                            # Build YouTube URL
                            full_url = f"https://www.youtube.com/watch?v={converted_base64}"
                            rows_to_update.append((idx, converted_base64, full_url))
            
            if not rows_to_update:
                logger.info("  No rows need Hex→Base64 conversion.")
                return 0
            
            logger.info(f"  Converting {len(rows_to_update)} rows...")
            
            # Process in larger chunks for efficiency (500 rows at a time)
            chunk_size = 500
            total = len(rows_to_update)
            
            for start_idx in range(0, total, chunk_size):
                end_idx = min(start_idx + chunk_size, total)
                chunk = rows_to_update[start_idx:end_idx]
                
                # Build single batch update for C:D columns
                updates = []
                for row_num, base64_code, youtube_url in chunk:
                    updates.append({
                        'range': f'C{row_num}:D{row_num}',
                        'values': [[base64_code, youtube_url]]
                    })
                
                # Execute batch update
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        clean_sheet.batch_update(updates, value_input_option='RAW')
                        break
                    except Exception as e:
                        logger.warning(f"Batch update attempt {attempt} failed: {e}")
                        if attempt == MAX_RETRIES:
                            raise
                        time.sleep(RETRY_DELAY * attempt)
                
                logger.info(f"  Converted {end_idx}/{total} rows...")
                time.sleep(0.3)
            
            logger.info(f"  ✓ Converted {len(rows_to_update)} rows (Hex → Base64 → YouTube URL)")
            return len(rows_to_update)
            
        except Exception as e:
            logger.error(f"Failed to convert hex to YouTube URLs: {e}")
            logger.debug(traceback.format_exc())
            return 0
    
    def get_existing_video_ids(self):
        """Read existing data from Clean data sheet - Video IDs and rows missing views"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            try:
                clean_sheet = master.worksheet('Clean data')
                # Get all values
                all_data = clean_sheet.get_all_values()
                
                # Clean data columns:
                # B (index 1): Video ID
                # D (index 3): Full Youtube links
                # E (index 4): App Link
                # H (index 7): Yt Views
                existing_video_ids = set()
                rows_missing_views = []  # List of (row_number, video_id) that need stats
                
                last_data_row = 1 # Header is row 1
                
                for idx, row in enumerate(all_data[1:], start=2):  # Start at row 2 (skip header)
                    video_id = row[1] if len(row) > 1 else ''  # Column B
                    youtube_url = row[3] if len(row) > 3 else ''  # Column D
                    app_link = row[4] if len(row) > 4 else '' # Column E
                    yt_views = row[7] if len(row) > 7 else ''  # Column H
                    
                    # Determine Video ID to use for stats (prefer B, fallback to D)
                    vid = video_id or self.extract_video_id(youtube_url)
                    
                    if vid:
                        existing_video_ids.add(vid)
                        
                        # Check if this row is missing views
                        if not yt_views or yt_views.strip() == '':
                            rows_missing_views.append((idx, vid))
                    
                    # Update last non-empty row tracking (check B and E)
                    if (video_id and video_id.strip()) or (app_link and app_link.strip()):
                        last_data_row = idx
                
                logger.info(f"✓ Found {len(existing_video_ids)} existing Video IDs in Clean data")
                logger.info(f"  - Rows missing views: {len(rows_missing_views)}")
                logger.info(f"  - Last row with data: {last_data_row}")
                
                return existing_video_ids, last_data_row, rows_missing_views
                
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
        
        # Prepare batch updates
        video_ids_to_fetch = []
        row_video_map = {}  # video_id -> list of row numbers
        
        for row_num, video_id in rows_missing_views:
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
            for row_num, video_id in rows_missing_views:
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
    
    def write_to_clean_data(self, data_rows, existing_video_ids, current_row_count):
        """Append new cleaned data to 'Clean data' tab (incremental, no clearing)"""
        try:
            master = self.client.open_by_key(self.master_sheet_id)
            
            # Filter out rows that already exist in Clean data (by Video ID)
            new_rows = []
            for row in data_rows:
                youtube_url = row[0]  # YouTube URL is first column in output
                video_id = self.extract_video_id(youtube_url)
                if video_id and video_id not in existing_video_ids:
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
            # ============================================
            # STEP 1: Read Combined Data and filter NEW rows
            # ============================================
            logger.info("Step 1: Reading and filtering Combined Data...")
            
            # Read existing Video IDs from Clean data
            existing_video_ids, current_row_count, _ = self.get_existing_video_ids()
            
            # Read Combined Data
            data_rows = self.read_combined_data()
            
            if not data_rows:
                logger.warning("No data in Combined Data. Exiting.")
                return
            
            # Filter to get only NEW valid rows (without YouTube API calls)
            cleaned_rows = self.clean_data_no_youtube(data_rows, existing_video_ids)
            
            # ============================================
            # STEP 2: Write NEW rows to Clean data (no YouTube stats yet)
            # ============================================
            new_count = 0
            if cleaned_rows:
                logger.info(f"Step 2: Writing {len(cleaned_rows)} new rows to Clean data...")
                new_count = self.write_new_rows(cleaned_rows, current_row_count)
            else:
                logger.info("Step 2: No new rows to add - all data already exists")
            
            # ============================================
            # STEP 3: Convert Hex IDs to Base64 and YouTube URLs
            # ============================================
            converted_count = self.convert_hex_to_youtube_urls()
            
            # ============================================
            # STEP 4: Update ALL rows missing YouTube views
            # ============================================
            logger.info("Step 4: Checking for rows missing YouTube views...")
            
            # Re-read Clean data to get rows missing views (includes newly added rows)
            _, _, rows_missing_views = self.get_existing_video_ids()
            
            updated_count = 0
            if rows_missing_views:
                logger.info(f"Found {len(rows_missing_views)} rows missing views. Fetching YouTube stats...")
                updated_count = self.update_missing_views(rows_missing_views)
            else:
                logger.info("All rows have YouTube views - nothing to update")
            
            # Summary
            elapsed = time.time() - start_time
            api_calls = (len(rows_missing_views) + 49) // 50 if rows_missing_views else 0
            
            logger.info("=" * 60)
            logger.info(f"✓ SUCCESS!")
            logger.info(f"  Combined Data rows: {len(data_rows)}")
            logger.info(f"  Already in Clean data: {len(existing_video_ids)}")
            logger.info(f"  NEW rows added: {new_count}")
            logger.info(f"  Hex→Base64 conversions: {converted_count}")
            logger.info(f"  Rows updated with YouTube stats: {updated_count}")
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
