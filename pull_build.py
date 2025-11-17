"""
Database creation, Google Sheets data pulling, and database population functions
for the Therapist Dashboard application.
"""

import pandas as pd
import json
import gspread
from google.oauth2.service_account import Credentials
import sqlite3 as sql
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


def init_database():
    """Initialize SQLite database with required tables matching Google Sheets structure"""
    db_path = "therapist_dashboard.db"
    conn = sql.connect(db_path)
    
    # Clients table - matches exact structure
    conn.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            ID TEXT PRIMARY KEY,
            counsellor_assn TEXT,
            age INTEGER,
            gender TEXT,
            client_type TEXT,
            county TEXT
        )
    ''')
    
    # Edinburgh Postnatal Depression Scale (EPDS)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS epds_responses (
            timestamp TEXT,
            client_code TEXT,
            epds_total_score INTEGER,
            severity_descriptor TEXT,
            item_10_raw_score INTEGER,
            suicidality_flag TEXT,
            column_1 TEXT,
            FOREIGN KEY (client_code) REFERENCES clients (ID)
        )
    ''')
    
    # Beck's Depression Inventory (BDI)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS bdi_responses (
            timestamp TEXT,
            client_code TEXT,
            bdi_total INTEGER,
            severity_level TEXT,
            clinical_interpretation TEXT,
            FOREIGN KEY (client_code) REFERENCES clients (ID)
        )
    ''')
    
    # Beck Anxiety Inventory (BAI)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS bai_responses (
            timestamp TEXT,
            client_code TEXT,
            total_score INTEGER,
            severity TEXT,
            clinical_conclusion TEXT,
            FOREIGN KEY (client_code) REFERENCES clients (ID)
        )
    ''')
    
    # ACE-Q Responses
    conn.execute('''
        CREATE TABLE IF NOT EXISTS aceq_responses (
            timestamp TEXT,
            client_code TEXT,
            total_ace_score INTEGER,
            FOREIGN KEY (client_code) REFERENCES clients (ID)
        )
    ''')
    
    # SADS Responses
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sads_responses (
            timestamp TEXT,
            client_code TEXT,
            social_avoidance_score INTEGER,
            social_avoidance_level TEXT,
            social_distress_score INTEGER,
            social_distress_level TEXT,
            total_sads_score INTEGER,
            overall_level TEXT,
            FOREIGN KEY (client_code) REFERENCES clients (ID)
        )
    ''')
    
    # ASRS Responses
    conn.execute('''
        CREATE TABLE IF NOT EXISTS asrs_responses (
            timestamp TEXT,
            client_code TEXT,
            part_a_score INTEGER,
            part_a_descriptor TEXT,
            part_b_score INTEGER,
            part_b_descriptor TEXT,
            total_score INTEGER,
            total_descriptor TEXT,
            inattentive_subscale_raw INTEGER,
            inattentive_subscale_percent REAL,
            hyperactivity_motor_subscale_raw INTEGER,
            hyperactivity_motor_subscale_percent REAL,
            hyperactivity_verbal_subscale_raw INTEGER,
            hyperactivity_verbal_subscale_percent REAL,
            FOREIGN KEY (client_code) REFERENCES clients (ID)
        )
    ''')
    
    # Sheet configuration table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sheet_config (
            sheet_name TEXT PRIMARY KEY,
            table_name TEXT,
            is_excluded INTEGER DEFAULT 0
        )
    ''')
    
    conn.commit()
    return conn


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


def check_database_populated():
    """Check if database already has data"""
    try:
        conn = sql.connect("therapist_dashboard.db")
        result = conn.execute("SELECT COUNT(*) FROM clients").fetchone()
        client_count = result[0] if result else 0
        conn.close()
        return client_count > 0
    except:
        return False


def populate_database_with_sheets(sheets_data):
    """Populate database with exact sheet structures"""
    conn = sql.connect("therapist_dashboard.db")
    
    # Clear existing data
    conn.execute("DELETE FROM clients")
    conn.execute("DELETE FROM epds_responses") 
    conn.execute("DELETE FROM bdi_responses")
    conn.execute("DELETE FROM bai_responses")
    conn.execute("DELETE FROM aceq_responses")
    conn.execute("DELETE FROM sads_responses")
    conn.execute("DELETE FROM asrs_responses")
    conn.execute("DELETE FROM sheet_config")
    
    # Sheet name to table name mapping
    sheet_table_mapping = {
        "Clients": "clients",
        "Edinburgh Postnatal Depression Scale (EPDS) (Responses) - EPDS Scoring": "epds_responses",
        "Beck's Depression Inventory (BDI) (Responses) - BDI Scoring": "bdi_responses", 
        "Beck Anxiety Inventory (BAI) (Responses) - BAI Scoring": "bai_responses",
        "ACE-Q Responses - ACE-Q Scoring": "aceq_responses",
        "SADS Responses - SADS Scoring": "sads_responses",
        "ASRS Responses - ASRS Scoring": "asrs_responses"
    }
    
    # Populate sheet config table
    excluded_list = ["Assessment Tools", "Generated Links", "Clients"]
    for sheet_name in sheets_data.keys():
        is_excluded = 1 if sheet_name in excluded_list else 0
        table_name = sheet_table_mapping.get(sheet_name, "")
        
        conn.execute(
            "INSERT OR REPLACE INTO sheet_config (sheet_name, table_name, is_excluded) VALUES (?, ?, ?)",
            (sheet_name, table_name, is_excluded)
        )
    
    # Populate clients table
    if "Clients" in sheets_data:
        clients_df = sheets_data["Clients"]
        valid_rows = 0
        for _, row in clients_df.iterrows():
            if pd.notna(row.get("ID")) and pd.notna(row.get("Counsellor Assn`")):
                valid_rows += 1
                conn.execute(
                    "INSERT OR REPLACE INTO clients (ID, counsellor_assn, age, gender, client_type, county) VALUES (?, ?, ?, ?, ?, ?)",
                    (safe_str(row.get("ID", "")), 
                    safe_str(row.get("Counsellor Assn`", "")),
                    safe_int(row.get("Age", "")),
                    safe_str(row.get("Gender", "")),
                    safe_str(row.get("Client Type", "")),
                    safe_str(row.get("county", "")))
                )
        logger.info(f"Populated clients table with {valid_rows} records")
    
    # Populate EPDS responses
    epds_sheet = "Edinburgh Postnatal Depression Scale (EPDS) (Responses) - EPDS Scoring"
    if epds_sheet in sheets_data and not sheets_data[epds_sheet].empty:
        for _, row in sheets_data[epds_sheet].iterrows():
            conn.execute(
                "INSERT INTO epds_responses (timestamp, client_code, epds_total_score, severity_descriptor, item_10_raw_score, suicidality_flag, column_1) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (parse_timestamp(row.get("Timestamp", "")),
                safe_str(row.get("Client Code", "")),
                safe_int(row.get("EPDS Total Score (Max 30)", "")),
                safe_str(row.get("Severity Descriptor", "")),
                safe_int(row.get("Item 10 (Harming Self) Raw Score", "")),
                safe_str(row.get("Suicidality Flag (Clinical Alert)", "")),
                safe_str(row.get("Column 1", "")))
            )
    
    # Populate BDI responses  
    bdi_sheet = "Beck's Depression Inventory (BDI) (Responses) - BDI Scoring"
    if bdi_sheet in sheets_data and not sheets_data[bdi_sheet].empty:
        for _, row in sheets_data[bdi_sheet].iterrows():
            conn.execute(
                "INSERT INTO bdi_responses (timestamp, client_code, bdi_total, severity_level, clinical_interpretation) VALUES (?, ?, ?, ?, ?)",
                (parse_timestamp(row.get("Timestamp", "")),
                safe_str(row.get("Client Code", "")),
                safe_int(row.get("BDI Total", "")),
                safe_str(row.get("Severity Level", "")),
                safe_str(row.get("Clinical Interpretation", "")))
            )
    
    # Populate BAI responses
    bai_sheet = "Beck Anxiety Inventory (BAI) (Responses) - BAI Scoring"  
    if bai_sheet in sheets_data and not sheets_data[bai_sheet].empty:
        for _, row in sheets_data[bai_sheet].iterrows():
            conn.execute(
                "INSERT INTO bai_responses (timestamp, client_code, total_score, severity, clinical_conclusion) VALUES (?, ?, ?, ?, ?)",
                (parse_timestamp(row.get("Timestamp", "")),
                safe_str(row.get("Client Code", "")),
                safe_int(row.get("Total Score", "")),
                safe_str(row.get("Severity", "")),
                safe_str(row.get("Clinical Conclusion ", "")))
            )
    
    # Populate ACE-Q responses
    aceq_sheet = "ACE-Q Responses - ACE-Q Scoring"
    if aceq_sheet in sheets_data and not sheets_data[aceq_sheet].empty:
        for _, row in sheets_data[aceq_sheet].iterrows():
            conn.execute(
                "INSERT INTO aceq_responses (timestamp, client_code, total_ace_score) VALUES (?, ?, ?)",
                (parse_timestamp(row.get("Timestamp", "")),
                safe_str(row.get("Client Code", "")),
                safe_int(row.get("Total ACE Score", "")))
            )
    
    # Populate SADS responses
    sads_sheet = "SADS Responses - SADS Scoring"
    if sads_sheet in sheets_data and not sheets_data[sads_sheet].empty:
        for _, row in sheets_data[sads_sheet].iterrows():
            conn.execute(
                "INSERT INTO sads_responses (timestamp, client_code, social_avoidance_score, social_avoidance_level, social_distress_score, social_distress_level, total_sads_score, overall_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (parse_timestamp(row.get("Timestamp", "")),
                safe_str(row.get("Client Code", "")),
                safe_int(row.get("Social Avoidance Score", "")),
                safe_str(row.get("Social Avoidance Level", "")),
                safe_int(row.get("Social Distress Score", "")),
                safe_str(row.get("Social Distress Level", "")),
                safe_int(row.get("Total SADS Score", "")),
                safe_str(row.get("Overall Level", "")))
            )
    
    # Populate ASRS responses  
    asrs_sheet = "ASRS Responses - ASRS Scoring"
    if asrs_sheet in sheets_data and not sheets_data[asrs_sheet].empty:
        for _, row in sheets_data[asrs_sheet].iterrows():
            conn.execute(
                "INSERT INTO asrs_responses (timestamp, client_code, part_a_score, part_a_descriptor, part_b_score, part_b_descriptor, total_score, total_descriptor, inattentive_subscale_raw, inattentive_subscale_percent, hyperactivity_motor_subscale_raw, hyperactivity_motor_subscale_percent, hyperactivity_verbal_subscale_raw, hyperactivity_verbal_subscale_percent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (parse_timestamp(row.get("Timestamp", "")),
                safe_str(row.get("Client Code", "")),
                safe_int(row.get("Part A Score", "")),
                safe_str(row.get("Part A Descriptor", "")),
                safe_int(row.get("Part B Score", "")),
                safe_str(row.get("Part B Descriptor", "")),
                safe_int(row.get("Total Score", "")),
                safe_str(row.get("Total Descriptor", "")),
                safe_int(row.get("Inattentive Subscale (Raw)", "")),
                parse_percentage(row.get("Inattentive Subscale (%)")),
                safe_int(row.get("Hyperactivity-Motor Subscale (Raw)", "")),
                parse_percentage(row.get("Hyperactivity-Motor Subscale (%)")),
                safe_int(row.get("Hyperactivity-Verbal Subscale (Raw)", "")),
                parse_percentage(row.get("Hyperactivity-Verbal Subscale (%)")))
            )
    
    conn.commit()
    conn.close()


def full_data_refresh():
    """Complete data refresh: initialize DB, load sheets, populate database"""
    # Initialize database
    db_conn = init_database()
    
    # Load data from Google Sheets
    all_tabs = load_spreadsheet_data()
    
    # Populate database with sheet data
    populate_database_with_sheets(all_tabs)
    
    db_conn.close()
    return all_tabs


if __name__ == "__main__":
    # For testing purposes - run data refresh
    logger.info("Starting data refresh...")
    data = full_data_refresh()
    logger.info(f"Data refresh complete! Loaded {len(data)} sheets.")
    for sheet_name, df in data.items():
        logger.info(f"  - {sheet_name}: {df.shape[0]} rows, {df.shape[1]} columns")