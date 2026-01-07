# --- MySQL connection config (update with your credentials)
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'therapist_user',
    'password': 'therapist_pass',
    'database': 'therapist_dashboard'
}

# --- MySQL connection helper
def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)

# --- Initialize MySQL schema
def init_database():
    conn = get_mysql_connection()
    cursor = conn.cursor()
    # Create tables if not exist (MySQL syntax)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            ID VARCHAR(64) PRIMARY KEY,
            counsellor_assn VARCHAR(255),
            age INT,
            gender VARCHAR(32),
            client_type VARCHAR(64),
            county VARCHAR(64)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS epds_responses (
            timestamp DATETIME,
            client_code VARCHAR(64),
            epds_total_score INT,
            severity_descriptor VARCHAR(64),
            item_10_raw_score INT,
            suicidality_flag VARCHAR(64),
            column_1 VARCHAR(64),
            FOREIGN KEY (client_code) REFERENCES clients(ID)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bdi_responses (
            timestamp DATETIME,
            client_code VARCHAR(64),
            bdi_total INT,
            severity_level VARCHAR(64),
            clinical_interpretation VARCHAR(255),
            FOREIGN KEY (client_code) REFERENCES clients(ID)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bai_responses (
            timestamp DATETIME,
            client_code VARCHAR(64),
            total_score INT,
            severity VARCHAR(64),
            clinical_conclusion VARCHAR(255),
            FOREIGN KEY (client_code) REFERENCES clients(ID)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aceq_responses (
            timestamp DATETIME,
            client_code VARCHAR(64),
            total_ace_score INT,
            FOREIGN KEY (client_code) REFERENCES clients(ID)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sads_responses (
            timestamp DATETIME,
            client_code VARCHAR(64),
            social_avoidance_score INT,
            social_avoidance_level VARCHAR(64),
            social_distress_score INT,
            social_distress_level VARCHAR(64),
            total_sads_score INT,
            overall_level VARCHAR(64),
            FOREIGN KEY (client_code) REFERENCES clients(ID)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS asrs_responses (
            timestamp DATETIME,
            client_code VARCHAR(64),
            part_a_score INT,
            part_a_descriptor VARCHAR(64),
            part_b_score INT,
            part_b_descriptor VARCHAR(64),
            total_score INT,
            total_descriptor VARCHAR(64),
            inattentive_subscale_raw INT,
            inattentive_subscale_percent FLOAT,
            hyperactivity_motor_subscale_raw INT,
            hyperactivity_motor_subscale_percent FLOAT,
            hyperactivity_verbal_subscale_raw INT,
            hyperactivity_verbal_subscale_percent FLOAT,
            FOREIGN KEY (client_code) REFERENCES clients(ID)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sheet_config (
            sheet_name VARCHAR(255) PRIMARY KEY,
            table_name VARCHAR(255),
            is_excluded TINYINT DEFAULT 0
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
# --- Required imports
import pandas as pd
import mysql.connector
import logging
from sheets_pull import load_spreadsheet_data
# --- Utility: Check if database is populated (MySQL)
def check_database_populated():
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM clients")
        result = cursor.fetchone()
        client_count = result[0] if result else 0
        cursor.close()
        conn.close()
        return client_count > 0
    except Exception:
        return False

# --- Utility: Full data refresh (init DB, load sheets, populate DB)
def full_data_refresh():
    init_database()
    all_tabs = load_spreadsheet_data()
    populate_database_with_sheets(all_tabs)
    return all_tabs
# Utility imports for type conversion
from pull_build import parse_percentage, safe_int, safe_float, safe_str, parse_timestamp

def populate_database_with_sheets(sheets_data):
    """Populate MySQL database with exact sheet structures from Google Sheets"""
    conn = get_mysql_connection()
    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("DELETE FROM epds_responses")
    cursor.execute("DELETE FROM bdi_responses")
    cursor.execute("DELETE FROM bai_responses")
    cursor.execute("DELETE FROM aceq_responses")
    cursor.execute("DELETE FROM sads_responses")
    cursor.execute("DELETE FROM asrs_responses")
    cursor.execute("DELETE FROM sheet_config")
    cursor.execute("DELETE FROM clients")


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
        cursor.execute(
            "REPLACE INTO sheet_config (sheet_name, table_name, is_excluded) VALUES (%s, %s, %s)",
            (sheet_name, table_name, is_excluded)
        )

    # Populate clients table
    if "Clients" in sheets_data:
        clients_df = sheets_data["Clients"]
        for _, row in clients_df.iterrows():
            if pd.notna(row.get("ID")) and pd.notna(row.get("Counsellor Assn`")):
                cursor.execute(
                    "REPLACE INTO clients (ID, counsellor_assn, age, gender, client_type, county) VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        safe_str(row.get("ID", "")),
                        safe_str(row.get("Counsellor Assn`", "")),
                        safe_int(row.get("Age", "")),
                        safe_str(row.get("Gender", "")),
                        safe_str(row.get("Client Type", "")),
                        safe_str(row.get("county", ""))
                    )
                )

    # Populate EPDS responses
    epds_sheet = "Edinburgh Postnatal Depression Scale (EPDS) (Responses) - EPDS Scoring"
    if epds_sheet in sheets_data and not sheets_data[epds_sheet].empty:
        for _, row in sheets_data[epds_sheet].iterrows():
            client_code = safe_str(row.get("Client Code", ""))
            cursor.execute("SELECT 1 FROM clients WHERE ID = %s", (client_code,))
            if cursor.fetchone():
                cursor.execute(
                    "INSERT INTO epds_responses (timestamp, client_code, epds_total_score, severity_descriptor, item_10_raw_score, suicidality_flag, column_1) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        parse_timestamp(row.get("Timestamp", "")),
                        client_code,
                        safe_int(row.get("EPDS Total Score (Max 30)", "")),
                        safe_str(row.get("Severity Descriptor", "")),
                        safe_int(row.get("Item 10 (Harming Self) Raw Score", "")),
                        safe_str(row.get("Suicidality Flag (Clinical Alert)", "")),
                        safe_str(row.get("Column 1", ""))
                    )
                )

    # Populate BDI responses
    bdi_sheet = "Beck's Depression Inventory (BDI) (Responses) - BDI Scoring"
    if bdi_sheet in sheets_data and not sheets_data[bdi_sheet].empty:
        for _, row in sheets_data[bdi_sheet].iterrows():
            client_code = safe_str(row.get("Client Code", ""))
            cursor.execute("SELECT 1 FROM clients WHERE ID = %s", (client_code,))
            if cursor.fetchone():
                cursor.execute(
                    "INSERT INTO bdi_responses (timestamp, client_code, bdi_total, severity_level, clinical_interpretation) VALUES (%s, %s, %s, %s, %s)",
                    (
                        parse_timestamp(row.get("Timestamp", "")),
                        client_code,
                        safe_int(row.get("BDI Total", "")),
                        safe_str(row.get("Severity Level", "")),
                        safe_str(row.get("Clinical Interpretation", ""))
                    )
                )

    # Populate BAI responses
    bai_sheet = "Beck Anxiety Inventory (BAI) (Responses) - BAI Scoring"
    if bai_sheet in sheets_data and not sheets_data[bai_sheet].empty:
        for _, row in sheets_data[bai_sheet].iterrows():
            client_code = safe_str(row.get("Client Code", ""))
            cursor.execute("SELECT 1 FROM clients WHERE ID = %s", (client_code,))
            if cursor.fetchone():
                cursor.execute(
                    "INSERT INTO bai_responses (timestamp, client_code, total_score, severity, clinical_conclusion) VALUES (%s, %s, %s, %s, %s)",
                    (
                        parse_timestamp(row.get("Timestamp", "")),
                        client_code,
                        safe_int(row.get("Total Score", "")),
                        safe_str(row.get("Severity", "")),
                        safe_str(row.get("Clinical Conclusion ", ""))
                    )
                )

    # Populate ACE-Q responses
    aceq_sheet = "ACE-Q Responses - ACE-Q Scoring"
    if aceq_sheet in sheets_data and not sheets_data[aceq_sheet].empty:
        for _, row in sheets_data[aceq_sheet].iterrows():
            client_code = safe_str(row.get("Client Code", ""))
            cursor.execute("SELECT 1 FROM clients WHERE ID = %s", (client_code,))
            if cursor.fetchone():
                cursor.execute(
                    "INSERT INTO aceq_responses (timestamp, client_code, total_ace_score) VALUES (%s, %s, %s)",
                    (
                        parse_timestamp(row.get("Timestamp", "")),
                        client_code,
                        safe_int(row.get("Total ACE Score", ""))
                    )
                )

    # Populate SADS responses
    sads_sheet = "SADS Responses - SADS Scoring"
    if sads_sheet in sheets_data and not sheets_data[sads_sheet].empty:
        for _, row in sheets_data[sads_sheet].iterrows():
            client_code = safe_str(row.get("Client Code", ""))
            cursor.execute("SELECT 1 FROM clients WHERE ID = %s", (client_code,))
            if cursor.fetchone():
                cursor.execute(
                    "INSERT INTO sads_responses (timestamp, client_code, social_avoidance_score, social_avoidance_level, social_distress_score, social_distress_level, total_sads_score, overall_level) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        parse_timestamp(row.get("Timestamp", "")),
                        client_code,
                        safe_int(row.get("Social Avoidance Score", "")),
                        safe_str(row.get("Social Avoidance Level", "")),
                        safe_int(row.get("Social Distress Score", "")),
                        safe_str(row.get("Social Distress Level", "")),
                        safe_int(row.get("Total SADS Score", "")),
                        safe_str(row.get("Overall Level", ""))
                    )
                )

    # Populate ASRS responses
    asrs_sheet = "ASRS Responses - ASRS Scoring"
    if asrs_sheet in sheets_data and not sheets_data[asrs_sheet].empty:
        for _, row in sheets_data[asrs_sheet].iterrows():
            client_code = safe_str(row.get("Client Code", ""))
            cursor.execute("SELECT 1 FROM clients WHERE ID = %s", (client_code,))
            if cursor.fetchone():
                cursor.execute(
                    "INSERT INTO asrs_responses (timestamp, client_code, part_a_score, part_a_descriptor, part_b_score, part_b_descriptor, total_score, total_descriptor, inattentive_subscale_raw, inattentive_subscale_percent, hyperactivity_motor_subscale_raw, hyperactivity_motor_subscale_percent, hyperactivity_verbal_subscale_raw, hyperactivity_verbal_subscale_percent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        parse_timestamp(row.get("Timestamp", "")),
                        client_code,
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
                        parse_percentage(row.get("Hyperactivity-Verbal Subscale (%)"))
                    )
                )

    conn.commit()
    cursor.close()
    conn.close()
