"""
checks.py
Houses all data-quality checks for the Therapist Dashboard.

Each public function accepts a tool DataFrame and returns a result that
app.py can render directly.  New checks should be added here as standalone
functions so they remain easy to test and extend.
"""

import pandas as pd


# ---------------------------------------------------------------------------
# Check 1 – Submissions less than 24 hours apart for the same client
# ---------------------------------------------------------------------------

def flag_close_submissions(tool_df: pd.DataFrame, threshold_hours: int = 24) -> pd.DataFrame:
    """Return a DataFrame of submission pairs where the same client submitted
    more than once within *threshold_hours* of a previous submission.

    Columns returned:
        Client Code   - the client identifier
        Timestamp     - the submission timestamp
        Hours Apart   - gap to the nearest earlier submission (rounded to 2 dp)
        Gap Label     - human-readable gap string, e.g. "3h 12m"
        Flag          - always "⚠️ < {threshold_hours}h apart"
        + all other columns from the original sheet

    Both rows of each offending pair are included so reviewers have full
    context (the earlier submission and the suspiciously close follow-up).
    """
    if (
        tool_df.empty
        or "Timestamp" not in tool_df.columns
        or "Client Code" not in tool_df.columns
    ):
        return pd.DataFrame()

    df = tool_df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.dropna(subset=["Timestamp"])

    if df.empty:
        return pd.DataFrame()

    threshold = pd.Timedelta(hours=threshold_hours)
    flagged_indices: set = set()
    gap_map: dict = {}          # index → timedelta to nearest earlier submission

    for _client, group in df.groupby("Client Code"):
        sorted_ts = group["Timestamp"].sort_values()
        diffs = sorted_ts.diff().abs()          # gap to the previous submission
        close = diffs[diffs < threshold]

        for idx in close.index:
            gap_map[idx] = diffs[idx]
            flagged_indices.add(idx)

            # Also include the earlier row in the pair
            pos = sorted_ts.index.get_loc(idx)
            if pos > 0:
                prev_idx = sorted_ts.index[pos - 1]
                flagged_indices.add(prev_idx)
                # The earlier row gets the same gap for display purposes
                if prev_idx not in gap_map:
                    gap_map[prev_idx] = diffs[idx]

    if not flagged_indices:
        return pd.DataFrame()

    result = df.loc[sorted(flagged_indices)].copy()

    # Compute display columns
    def _fmt_gap(td: pd.Timedelta) -> str:
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes = remainder // 60
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"

    result["Hours Apart"] = result.index.map(
        lambda i: round(gap_map[i].total_seconds() / 3600, 2) if i in gap_map else None
    )
    result["Gap Label"] = result.index.map(
        lambda i: _fmt_gap(gap_map[i]) if i in gap_map else ""
    )
    result["Flag"] = f"< {threshold_hours}h apart"

    # Tidy: move the new diagnostic columns to the front
    front_cols = ["Client Code", "Timestamp", "Hours Apart", "Gap Label", "Flag"]
    other_cols = [c for c in result.columns if c not in front_cols]
    result = result[front_cols + other_cols].reset_index(drop=True)

    return result


def render_close_submissions(tool_df: pd.DataFrame, threshold_hours: int = 24) -> None:
    """Streamlit renderer for the close-submissions check.

    Call this directly from app.py after the Plotly chart.  Renders nothing
    if no flagged rows are found.
    """
    import streamlit as st

    flagged = flag_close_submissions(tool_df, threshold_hours=threshold_hours)

    if flagged.empty:
        return

    n_clients = flagged["Client Code"].nunique()
    n_pairs   = len(flagged)

    with st.expander(
        f"{n_clients} client(s) have submissions less than {threshold_hours}h apart "
        f"— {n_pairs} row(s) flagged",
        expanded=False,
    ):
        st.caption(
            f"The rows below each involve a client who submitted this tool more than once "
            f"within **{threshold_hours} hours**. Both submissions in each offending pair "
            f"are shown. Please verify these are intentional."
        )
        st.dataframe(flagged, width="stretch", hide_index=True)
