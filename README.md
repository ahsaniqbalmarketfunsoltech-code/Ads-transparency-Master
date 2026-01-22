# Ads Transparency Data Pipeline

This repository automates the extraction, cleaning, and analysis of ads transparency data using Google Sheets, Python, and Google BigQuery.

## 🚀 Overview

The pipeline consists of two main stages that run automatically via GitHub Actions:

1.  **Combine Data**: Aggregates raw data from multiple source Google Sheets into a single "Combined Data" sheet.
2.  **Clean & Sync**: Reads the combined data, filters for valid app links, fetches YouTube video statistics, and syncs the specific clean dataset directly to **BigQuery**.

**Data Flow:**
`Source Sheets` -> `Master Sheet (Combined)` -> `BigQuery (Cleaned + Stats)` -> `Looker Studio`

---

## 🛠️ Components

### 1. `combine_sheets.py`
*   **Trigger**: Runs hourly (at minute 0).
*   **Function**:
    *   Reads source sheet links from the 'Sources' tab.
    *   Fetches raw data from all sources.
    *   Merges them into the 'Combined Data' tab in the master spreadsheet.
    *   Handles duplicate columns and format discrepancies.

### 2. `clean_data.py`
*   **Trigger**: Runs hourly (at minute 30).
*   **Function**:
    *   Reads data from 'Combined Data'.
    *   Filters for valid Play Store links.
    *   Fetches YouTube View Counts and Upload Dates for new videos via the YouTube Data API.
    *   **Upserts (Update/Insert)** the data into Google BigQuery.

### 3. GitHub Workflows
*   `combine-sheets.yml`: Orchestrates the combiner script.
*   `clean-data.yml`: Orchestrates the cleaner and BigQuery sync script.

---

## ⚙️ Setup & Configuration

### Prerequisites
*   Google Cloud Project with **BigQuery API** enabled.
*   Service Account with permissions:
    *   `BigQuery Data Editor`
    *   `BigQuery Job User`
    *   `Google Drive / Sheets Editor`

### Environment Variables (GitHub Secrets)
Set the following secrets in your GitHub repository:

| Secret Name | Description |
| :--- | :--- |
| `GOOGLE_CREDENTIALS` | The JSON content of your Google Service Account Key. |
| `MASTER_SPREADSHEET_ID` | The ID of your main Google Sheet acting as the controller. |
| `YOUTUBE_API_KEY` | (Optional) API Key for YouTube Data API v3. Uses default if omitted. |

### BigQuery Schema
The script automatically creates the Dataset (`ads_data_staging`) and Table (`clean_ads_transparency`) if they don't exist.

**Table Schema:**
*   `video_id` (STRING, Required)
*   `youtube_url` (STRING)
*   `app_link` (STRING)
*   `app_name` (STRING)
*   `advertiser_name` (STRING)
*   `views` (INTEGER)
*   `upload_time` (STRING)
*   `last_updated` (TIMESTAMP, Partitioned)

---

## 📊 Analytics
Connect **Looker Studio** or **Tableau** directly to the BigQuery table `ads_data_staging.clean_ads_transparency` for real-time, high-performance dashboards.
