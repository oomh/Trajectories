"""
Shared utility functions for Therapist Dashboard (type conversions, etc).
"""

import pandas as pd
import json
import gspread
from google.oauth2.service_account import Credentials
import streamlit as st
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_percentage(value):
    """Parse percentage string to float (e.g., '67%' -> 67.0)"""
    if pd.notna(value):
        value_str = str(value).strip()
        if value_str.endswith('%'):
            try:
                return float(value_str[:-1])
            except ValueError:
                return None
        else:
            try:
                return float(value_str)
            except ValueError:
                return None
    return None


def safe_int(value):
    """Safely convert value to integer, return None if invalid"""
    if pd.notna(value) and str(value).strip() != "":
        try:
            return int(float(str(value).strip()))
        except (ValueError, TypeError):
            return None
    return None


def safe_float(value):
    """Safely convert value to float, return None if invalid"""
    if pd.notna(value) and str(value).strip() != "":
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return None
    return None


def safe_str(value):
    """Safely convert value to string, return empty string if null"""
    return str(value).strip() if pd.notna(value) else ""


def parse_timestamp(value):
    """Parse timestamp to standardized format"""
    if pd.notna(value) and str(value).strip() != "":
        try:
            # Convert to pandas datetime and back to string for standardization
            dt = pd.to_datetime(str(value).strip())
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return str(value).strip()
    return None




def load_spreadsheet_data():
    """Load and cache spreadsheet data from Google Sheets"""
    # Load service account credentials from secrets.toml
    service_account_info = json.loads(st.secrets["google_sheets"]["service_account_json"], strict=False)
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    
    # Authorize gspread
    client = gspread.authorize(creds)
    
    # Open spreadsheet by URL

    sheet_url = st.secrets["google_sheets"]["sheet_url"]
        
    spreadsheet = client.open_by_url(sheet_url)
    
    # Read ALL tabs into a dict: {tab_name: dataframe}
    all_tabs = {}
    for worksheet in spreadsheet.worksheets():
        tab_name = worksheet.title
        records = worksheet.get_all_records()
        df = pd.DataFrame(records)
        all_tabs[tab_name] = df
        
        # Log columns for this sheet
        logger.info(f"Sheet '{tab_name}' imported with {len(df)} rows and columns: {list(df.columns)}")
    
    return all_tabs








