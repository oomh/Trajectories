"""
sheets_data.py
Handles data processing directly from Google Sheets (no database needed).
Suitable for Streamlit Cloud deployment.
"""

import pandas as pd
import streamlit as st
from sheets_pull import load_spreadsheet_data

@st.cache_data(ttl=3600)
def get_all_sheets_data():
    """Load all data from Google Sheets - cached for 1 hour"""
    return load_spreadsheet_data()


def get_available_tools(all_sheets):
    """Get list of available assessment tools (exclude administrative sheets)"""
    excluded = ["Assessment Tools", "Generated Links", "Clients"]
    tools = [sheet for sheet in all_sheets.keys() if sheet not in excluded]
    return sorted(tools)


def get_therapist_list(clients_df):
    """Get unique list of therapists"""
    therapists = clients_df['Counsellor Assn`'].dropna().unique()
    return sorted(therapists.tolist())


def get_clients_for_therapist(clients_df, therapist_name):
    """Filter clients by therapist"""
    if therapist_name == "All":
        return clients_df
    return clients_df[clients_df['Counsellor Assn`'] == therapist_name]


def get_therapist_comprehensive_counts(all_sheets):
    """Get client counts per therapist across all tools"""
    clients_df = all_sheets.get("Clients", pd.DataFrame())
    
    if clients_df.empty:
        return []
    
    results = []
    
    for therapist in get_therapist_list(clients_df):
        therapist_clients = get_clients_for_therapist(clients_df, therapist)
        client_ids = set(therapist_clients['ID'].unique())
        
        # Count clients per tool
        tool_counts = {}
        for tool_sheet in get_available_tools(all_sheets):
            tool_df = all_sheets.get(tool_sheet, pd.DataFrame())
            if not tool_df.empty and "Client Code" in tool_df.columns:
                tool_clients = set(tool_df['Client Code'].dropna().unique())
                tool_counts[tool_sheet] = len(client_ids & tool_clients)
            else:
                tool_counts[tool_sheet] = 0
        
        results.append({
            'therapist': therapist,
            'total_clients': len(client_ids),
            'tool_counts': tool_counts
        })
    
    return results


def get_client_trajectory_data(all_sheets, client_code, tool_name):
    """Get trajectory data for a specific client and tool"""
    tool_df = all_sheets.get(tool_name, pd.DataFrame())
    
    if tool_df.empty:
        return pd.DataFrame()
    
    # Filter for this client
    client_data = tool_df[tool_df['Client Code'] == client_code].copy()
    
    if client_data.empty:
        return pd.DataFrame()
    
    # Parse timestamp if it exists
    if 'Timestamp' in client_data.columns:
        try:
            client_data['Timestamp'] = pd.to_datetime(client_data['Timestamp'])
            client_data = client_data.sort_values('Timestamp')
        except Exception:
            pass
    
    return client_data
