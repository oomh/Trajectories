"""
Handles pulling data from Google Sheets for the Therapist Dashboard application.
"""

import pandas as pd
import json
import gspread
from google.oauth2.service_account import Credentials
import streamlit as st
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_spreadsheet_data():
    """Load and cache spreadsheet data from Google Sheets"""
    service_account_info = json.loads(st.secrets["google_sheets"]["service_account_json"], strict=False)
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    client = gspread.authorize(creds)
    sheet_url = st.secrets["google_sheets"]["sheet_url"]
    spreadsheet = client.open_by_url(sheet_url)
    all_tabs = {}
    for worksheet in spreadsheet.worksheets():
        tab_name = worksheet.title
        records = worksheet.get_all_records()
        df = pd.DataFrame(records)
        all_tabs[tab_name] = df
        logger.info(f"Sheet '{tab_name}' imported with {len(df)} rows and columns: {list(df.columns)}")
    return all_tabs
