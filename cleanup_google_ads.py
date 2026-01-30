"""
Cleanup script to remove messy data from the daily data experiment.
This script will:
1. Delete rows where app_name is null (these were inserted by the daily data MERGE)
2. Reset all google_ads columns to NULL for remaining rows
3. After running this, run google_ads_sync.py to get fresh data
"""

import os
import json
from google.cloud import bigquery

def cleanup():
    # Get credentials
    creds_json = os.getenv('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS required")
    
    creds_dict = json.loads(creds_json)
    bq_client = bigquery.Client.from_service_account_info(creds_dict)
    
    dataset_id = os.getenv('BIGQUERY_DATASET', 'ads_data_staging')
    table_id = os.getenv('BIGQUERY_TABLE', 'clean_ads_transparency')
    full_table_id = f"{bq_client.project}.{dataset_id}.{table_id}"
    
    print("=" * 50)
    print("🧹 Cleaning up messy data from daily experiment...")
    print("=" * 50)
    
    # Step 1: Delete rows where app_name is null (these were wrongly inserted)
    print("\n1️⃣ Deleting rows with null app_name (wrongly inserted rows)...")
    delete_query = f"""
        DELETE FROM `{full_table_id}`
        WHERE app_name IS NULL OR app_name = 'null' OR app_name = ''
    """
    result = bq_client.query(delete_query).result()
    print(f"   ✓ Deleted rows with null app_name")
    
    # Step 2: Reset all google_ads columns to NULL
    print("\n2️⃣ Resetting all google_ads columns to NULL...")
    reset_query = f"""
        UPDATE `{full_table_id}`
        SET 
            google_ads_impressions = NULL,
            google_ads_clicks = NULL,
            google_ads_cost = NULL,
            google_ads_conversions = NULL,
            google_ads_conversions_value = NULL,
            google_ads_video_views = NULL,
            google_ads_interactions = NULL,
            google_ads_all_conversions = NULL,
            google_ads_ctr = NULL,
            google_ads_avg_cpc = NULL,
            google_ads_avg_cpm = NULL,
            google_ads_cvc = NULL,
            google_ads_video_25_rate = NULL,
            google_ads_video_50_rate = NULL,
            google_ads_video_75_rate = NULL,
            google_ads_video_100_rate = NULL,
            google_ads_campaign_name = NULL,
            google_ads_campaign_id = NULL,
            google_ads_campaign_status = NULL,
            google_ads_campaign_type = NULL,
            google_ads_campaign_subtype = NULL,
            google_ads_package_id = NULL,
            google_ads_app_store = NULL,
            google_ads_ad_group_name = NULL,
            google_ads_ad_group_id = NULL,
            google_ads_account_name = NULL,
            google_ads_account_id = NULL,
            google_ads_status = NULL,
            google_ads_approval_status = NULL,
            google_ads_review_status = NULL,
            google_ads_date = NULL,
            google_ads_last_sync = NULL
        WHERE 1=1
    """
    bq_client.query(reset_query).result()
    print("   ✓ Reset all google_ads columns to NULL")
    
    # Step 3: Count remaining rows
    count_query = f"SELECT COUNT(*) as total FROM `{full_table_id}`"
    count_result = list(bq_client.query(count_query).result())[0]
    print(f"\n3️⃣ Remaining rows: {count_result.total}")
    
    print("\n" + "=" * 50)
    print("✅ Cleanup complete!")
    print("=" * 50)
    print("\n👉 Now run: python google_ads_sync.py")

if __name__ == "__main__":
    cleanup()
