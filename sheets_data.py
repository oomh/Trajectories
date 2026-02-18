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


# ---------------------------------------------------------------------------
# Full-name lookup for each assessment tool
# ---------------------------------------------------------------------------
TOOL_FULL_NAMES = {
    "EPDS":  "Edinburgh Postnatal Depression Scale (EPDS)",
    "BDI":   "Beck's Depression Inventory (BDI)",
    "BAI":   "Beck's Anxiety Inventory (BAI)",
    "ACE-Q": "Adverse Childhood Experiences Questionnaire (ACE-Q)",
    "SADS":  "Social Avoidance and Distress Scale (SADS)",
    "ASRS":  "Adult ADHD Self-Report Scale v1.1 (ASRS)",
}


def _short_key(tool_name: str) -> str:
    """Return the canonical short key for a tool sheet name."""
    for key in TOOL_FULL_NAMES:
        if key in tool_name:
            return key
    return tool_name[:10]


def _score_col_for(tool_name: str, df: pd.DataFrame):
    """Return the primary score column name for a given tool sheet, or None."""
    candidates = {
        "EPDS": "EPDS Total Score (Max 30)",
        "BDI":  "BDI Total",
        "BAI":  "Total Score",
        "ACE-Q": "Total ACE Score",
        "SADS": "Total SADS Score",
        "ASRS": "Total Score",
    }
    for key, col in candidates.items():
        if key in tool_name and col in df.columns:
            return col
    return None


def get_global_summary(all_sheets):
    """
    Compute cross-tool and per-tool summary statistics.

    Returns a dict with:
        'global_unique_clients' : int  – unique Client Codes across all tool sheets
        'tools'                 : list of dicts, one per tool, each containing:
            'short_name'         : str
            'full_name'          : str
            'total_clients'      : int  – unique Client Codes in this tool sheet
            'one_timepoint'      : int  – clients with exactly 1 submission
            'multi_assessment'   : int  – clients with > 1 submission
            'avg_change_pct'     : float | None  – average % change from first→last score
    """
    tools = get_available_tools(all_sheets)

    all_client_codes: set = set()
    tool_stats = []

    for tool_name in tools:
        df = all_sheets.get(tool_name, pd.DataFrame())

        if df.empty or "Client Code" not in df.columns:
            short = _short_key(tool_name)
            tool_stats.append({
                "short_name": short,
                "full_name": TOOL_FULL_NAMES.get(short, tool_name),
                "total_clients": 0,
                "one_timepoint": 0,
                "multi_assessment": 0,
                "avg_change_pct": None,
            })
            continue

        codes = df["Client Code"].dropna()
        all_client_codes.update(codes.tolist())

        counts_per_client = codes.value_counts()
        one_tp = int((counts_per_client == 1).sum())
        multi = int((counts_per_client > 1).sum())
        total = int(counts_per_client.shape[0])

        # Average % change (first → last score for clients with ≥2 points)
        score_col = _score_col_for(tool_name, df)
        avg_change_pct = None
        if score_col:
            df_score = df.copy()
            df_score[score_col] = pd.to_numeric(df_score[score_col], errors="coerce")
            df_score = df_score.dropna(subset=[score_col, "Client Code"])

            # Use entry_number if available, otherwise fall back to row order
            sort_col = "entry_number" if "entry_number" in df_score.columns else df_score.index.name or "index"
            if sort_col == "index":
                df_score = df_score.reset_index()

            df_score = df_score.sort_values(["Client Code", sort_col])

            first_scores = df_score.groupby("Client Code")[score_col].first()
            last_scores  = df_score.groupby("Client Code")[score_col].last()

            # Only consider clients who have at least 2 data points
            clients_multi = counts_per_client[counts_per_client > 1].index
            first_m = first_scores.loc[first_scores.index.isin(clients_multi)]
            last_m  = last_scores.loc[last_scores.index.isin(clients_multi)]

            if not first_m.empty:
                # Avoid division-by-zero; drop zeros
                mask = first_m != 0
                if mask.any():
                    pct_changes = ((last_m[mask] - first_m[mask]) / first_m[mask] * 100)
                    avg_change_pct = float(pct_changes.mean())

        short = _short_key(tool_name)
        tool_stats.append({
            "short_name": short,
            "full_name": TOOL_FULL_NAMES.get(short, tool_name),
            "total_clients": total,
            "one_timepoint": one_tp,
            "multi_assessment": multi,
            "avg_change_pct": avg_change_pct,
        })

    return {
        "global_unique_clients": len(all_client_codes),
        "tools": tool_stats,
    }


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



