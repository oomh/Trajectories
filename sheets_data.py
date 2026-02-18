"""
sheets_data.py
Handles data processing directly from Google Sheets (no database needed).
Suitable for Streamlit Cloud deployment.
"""

import pandas as pd
import streamlit as st
from sheets_pull import load_spreadsheet_data

EXCLUDED_SHEETS = ["Assessment Tools", "Generated Links", "Clients"]

ACTIVE_THERAPISTS = {"Ian", "Peris", "Fred", "Wangui", "Sandra", "Kinywa", "Joy", "Debra"}


@st.cache_data(ttl=3600)
def get_all_sheets_data():
    """Load all data from Google Sheets - cached for 1 hour.
    
    After loading, each tool sheet gets an 'entry_number' column that
    numbers each client's submissions chronologically (1, 2, 3 …).
    """
    all_tabs = load_spreadsheet_data()

    # Filter Clients sheet to only active therapists
    if "Clients" in all_tabs and 'Counsellor Assn`' in all_tabs["Clients"].columns:
        all_tabs["Clients"] = all_tabs["Clients"][
            all_tabs["Clients"]['Counsellor Assn`'].isin(ACTIVE_THERAPISTS)
        ].reset_index(drop=True)

    # Primary lookup: Client Code -> Counsellor from Generated Links
    links_df = all_tabs.get("Generated Links", pd.DataFrame())
    if (
        not links_df.empty
        and 'Client Code' in links_df.columns
        and 'Counsellor' in links_df.columns
    ):
        id_therapist = (
            links_df[['Client Code', 'Counsellor']]
            .drop_duplicates(subset='Client Code')
            .rename(columns={'Counsellor': 'therapist'})
        )
    else:
        id_therapist = pd.DataFrame(columns=['Client Code', 'therapist'])

    # Fallback lookup: Client ID -> Counsellor Assn` from Clients sheet
    clients_df = all_tabs.get("Clients", pd.DataFrame())
    if (
        not clients_df.empty
        and 'ID' in clients_df.columns
        and 'Counsellor Assn`' in clients_df.columns
    ):
        id_therapist_fallback = (
            clients_df[['ID', 'Counsellor Assn`']]
            .drop_duplicates(subset='ID')
            .rename(columns={'ID': 'Client Code', 'Counsellor Assn`': 'therapist'})
        )
    else:
        id_therapist_fallback = pd.DataFrame(columns=['Client Code', 'therapist'])

    for sheet_name, df in all_tabs.items():
        if sheet_name in EXCLUDED_SHEETS:
            continue
        if 'Client Code' not in df.columns:
            continue

        # Parse Timestamp to datetime before sorting so year boundaries sort correctly
        # (string sort puts "1/2026" before "12/2025")
        if 'Timestamp' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], dayfirst=False, errors='coerce')
            df.sort_values(['Client Code', 'Timestamp'], inplace=True)
        else:
            df.sort_values(['Client Code'], inplace=True)
        df.reset_index(drop=True, inplace=True)

        df['entry_number'] = df.groupby('Client Code').cumcount() + 1

        # Add therapist column — primary: Generated Links, fallback: Clients sheet
        merged = df.merge(id_therapist, on='Client Code', how='left')
        df['therapist'] = merged['therapist'].values

        # Fill any NaNs from the fallback lookup
        missing = df['therapist'].isna()
        if missing.any():
            merged_fallback = df.loc[missing, ['Client Code']].merge(
                id_therapist_fallback, on='Client Code', how='left'
            )
            df.loc[missing, 'therapist'] = merged_fallback['therapist'].values

    return all_tabs


def get_available_tools(all_sheets):
    """Get list of available assessment tools (exclude administrative sheets)"""
    tools = [sheet for sheet in all_sheets.keys() if sheet not in EXCLUDED_SHEETS]
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


def get_tool_summary_counts(all_sheets):
    """For each tool sheet return total entries and unique client codes."""
    results = []
    for tool_name in get_available_tools(all_sheets):
        tool_df = all_sheets.get(tool_name, pd.DataFrame())
        if tool_df.empty or 'Client Code' not in tool_df.columns:
            total_entries = 0
            unique_clients = 0
        else:
            total_entries = len(tool_df)
            unique_clients = tool_df['Client Code'].dropna().nunique()

        # Short display name
        for key in ('EPDS', 'BDI', 'BAI', 'ACE-Q', 'SADS', 'ASRS'):
            if key in tool_name:
                short_name = key
                break
        else:
            short_name = tool_name[:10]

        results.append({
            'tool': short_name,
            'total_entries': total_entries,
            'unique_clients': unique_clients,
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


def get_duplicate_submissions(tool_df):
    """Return rows where the same client has two or more submissions
    within 24 hours of each other. The returned DataFrame includes both
    rows of each offending pair so reviewers have full context."""
    if tool_df.empty or 'Timestamp' not in tool_df.columns or 'Client Code' not in tool_df.columns:
        return pd.DataFrame()

    df = tool_df.copy()
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df = df.dropna(subset=['Timestamp'])

    flagged_indices = set()

    for client, group in df.groupby('Client Code'):
        timestamps = group['Timestamp'].sort_values()
        # Compare each submission to the next one
        diffs = timestamps.diff().abs()
        close = diffs[diffs < pd.Timedelta(hours=24)]
        for idx in close.index:
            # Flag both the current row and the one before it
            pos = timestamps.index.get_loc(idx)
            flagged_indices.add(idx)
            if pos > 0:
                flagged_indices.add(timestamps.index[pos - 1])

    if not flagged_indices:
        return pd.DataFrame()

    return df.loc[sorted(flagged_indices)].reset_index(drop=True)
