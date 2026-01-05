import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
from db_build import init_database, get_mysql_connection, check_database_populated, full_data_refresh
from sheets_pull import load_spreadsheet_data
from pull_build import parse_percentage, safe_int, safe_float, safe_str, parse_timestamp
from streamlit_extras.bottom_container import bottom 

st.set_page_config(page_title="Therapist Dashboard", layout="wide")


# Initialize database and load data only if needed
@st.cache_data
def get_cached_data():
    """Load data from Google Sheets - cached to avoid reloading"""
    return full_data_refresh()


# Get available tools from database (MySQL)
def get_available_tools():
    """Get non-excluded sheet names from database (MySQL)"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT sheet_name FROM sheet_config WHERE is_excluded = 0 ORDER BY sheet_name")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return [row[0] for row in result]


# Initialize database
init_database()

# Check if we need to refresh data

with st.columns(1)[0]:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Load data (cached or fresh)
if check_database_populated():
    st.success("✅ Using cached database data!")
    # For display purposes, we still need the tabs structure
    # but we'll load it from database in the functions below
    all_tabs = {}
else:
    with st.spinner("Loading fresh data from Google Sheets..."):
        all_tabs = get_cached_data()
        st.success("✅ Loaded fresh data from Google Sheets!")




available_tools = get_available_tools()


# Get comprehensive therapist client counts across all assessment tools
def get_therapist_comprehensive_counts():
    """Get detailed client counts per therapist across all assessment tools (MySQL)"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    query = """
    SELECT 
        c.counsellor_assn as therapist_name,
        COUNT(DISTINCT c.ID) as total_clients,
        COUNT(DISTINCT epds.client_code) as epds_clients,
        COUNT(DISTINCT bdi.client_code) as bdi_clients,
        COUNT(DISTINCT bai.client_code) as bai_clients,
        COUNT(DISTINCT aceq.client_code) as aceq_clients,
        COUNT(DISTINCT sads.client_code) as sads_clients,
        COUNT(DISTINCT asrs.client_code) as asrs_clients
    FROM clients c
    LEFT JOIN epds_responses epds ON c.ID = epds.client_code
    LEFT JOIN bdi_responses bdi ON c.ID = bdi.client_code
    LEFT JOIN bai_responses bai ON c.ID = bai.client_code
    LEFT JOIN aceq_responses aceq ON c.ID = aceq.client_code
    LEFT JOIN sads_responses sads ON c.ID = sads.client_code
    LEFT JOIN asrs_responses asrs ON c.ID = asrs.client_code
    WHERE c.counsellor_assn IS NOT NULL
    GROUP BY c.counsellor_assn
    ORDER BY c.counsellor_assn
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def get_therapist_client_counts():
    """Get basic therapist client counts for grid display (backwards compatibility)"""
    comprehensive_data = get_therapist_comprehensive_counts()
    # Return just therapist name and total count for grid compatibility
    return [(row[0], row[1]) for row in comprehensive_data]


def get_therapist_client_count(therapist_name):
    """Get total client count for a specific therapist or all therapists (MySQL)"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    if therapist_name == "All":
        cursor.execute("SELECT COUNT(DISTINCT ID) as total_clients FROM clients WHERE counsellor_assn IS NOT NULL")
        result = cursor.fetchone()
        client_count = result[0] if result else 0
    else:
        query = """
        SELECT COUNT(DISTINCT c.ID) as total_clients
        FROM clients c
        WHERE c.counsellor_assn = %s AND c.counsellor_assn IS NOT NULL
        """
        cursor.execute(query, (therapist_name,))
        result = cursor.fetchone()
        client_count = result[0] if result else 0
    cursor.close()
    conn.close()
    return client_count


def get_therapist_clients_for_tool(therapist_name, tool_name):
    """Get count of clients who have completed a specific assessment tool for a therapist"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    # Map tool names to their response tables
    tool_table_mapping = {
        "Clients": "clients",
        "Edinburgh Postnatal Depression Scale (EPDS) (Responses) - EPDS Scoring": "epds_responses",
        "Beck's Depression Inventory (BDI) (Responses) - BDI Scoring": "bdi_responses", 
        "Beck Anxiety Inventory (BAI) (Responses) - BAI Scoring": "bai_responses",
        "ACE-Q Responses - ACE-Q Scoring": "aceq_responses",
        "SADS Responses - SADS Scoring": "sads_responses",
        "ASRS Responses - ASRS Scoring": "asrs_responses"
    }
    
    # Get the table name for this tool
    table_name = None
    for tool_key, table_val in tool_table_mapping.items():
        if tool_key in tool_name or any(keyword in tool_name for keyword in ["EPDS", "BDI", "BAI", "ACE-Q", "SADS", "ASRS"]):
            if "EPDS" in tool_name:
                table_name = "epds_responses"
            elif "BDI" in tool_name:
                table_name = "bdi_responses"
            elif "BAI" in tool_name:
                table_name = "bai_responses"
            elif "ACE-Q" in tool_name:
                table_name = "aceq_responses"
            elif "SADS" in tool_name:
                table_name = "sads_responses"
            elif "ASRS" in tool_name:
                table_name = "asrs_responses"
            elif tool_name == "Clients":
                table_name = "clients"
            break
    
    if not table_name:
        cursor.close()
        conn.close()
        return 0
    try:
        if table_name == "clients":
            if therapist_name == "All":
                query = "SELECT COUNT(DISTINCT ID) FROM clients WHERE counsellor_assn IS NOT NULL"
                cursor.execute(query)
                result = cursor.fetchone()
            else:
                query = "SELECT COUNT(DISTINCT ID) FROM clients WHERE counsellor_assn = %s"
                cursor.execute(query, (therapist_name,))
                result = cursor.fetchone()
        else:
            if therapist_name == "All":
                query = f"""
                SELECT COUNT(DISTINCT r.client_code) 
                FROM {table_name} r 
                JOIN clients c ON r.client_code = c.ID 
                WHERE c.counsellor_assn IS NOT NULL
                """
                cursor.execute(query)
                result = cursor.fetchone()
            else:
                query = f"""
                SELECT COUNT(DISTINCT r.client_code) 
                FROM {table_name} r 
                JOIN clients c ON r.client_code = c.ID 
                WHERE c.counsellor_assn = %s
                """
                cursor.execute(query, (therapist_name,))
                result = cursor.fetchone()
        count = result[0] if result else 0
    except Exception as e:
        count = 0
    cursor.close()
    conn.close()
    return count


def get_clients_filtered():
    """Get filtered client data from database"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ID, counsellor_assn, gender, client_type FROM clients")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(result, columns=["ID", "Counsellor Assn", "Gender", "Client Type"])


def get_client_to_therapist_mapping():
    """Get client to therapist mapping from database"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ID, counsellor_assn FROM clients")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return {client_id: therapist for client_id, therapist in result}


def get_severity_ranges(tool_name):
    """Define severity ranges for each assessment tool"""
    severity_ranges = {
        "EPDS": {
            "Minimal": (0, 9, "lightgreen"),
            "Mild": (10, 12, "yellow"),
            "Moderate": (13, 21, "orange"),
            "Severe": (22, 30, "red"),
        },
        "BDI": {
            "Minimal": (0, 13, "lightgreen"),
            "Mild": (14, 19, "yellow"),
            "Moderate": (20, 28, "orange"),
            "Severe": (29, 63, "red"),
        },
        "BAI": {
            "Minimal": (0, 7, "lightgreen"),
            "Mild": (8, 15, "yellow"),
            "Moderate": (16, 25, "orange"),
            "Severe": (26, 63, "red"),
        },
        "SADS": {
            "Low": (0, 30, "lightgreen"),
            "Moderate": (31, 60, "orange"),
            "High": (61, 100, "red"),
        },
        "ACE-Q": {
            "Low Risk": (0, 3, "lightgreen"),
            "Moderate Risk": (4, 6, "orange"),
            "High Risk": (7, 10, "red"),
        },
        "ASRS": {
            "Low": (0, 40, "lightgreen"),
            "Moderate": (41, 60, "orange"),
            "High": (61, 100, "red"),
        },
    }

    for key in severity_ranges:
        if key in tool_name:
            return severity_ranges[key]
    return {}


def get_tool_data(tool_name):
    """Get data for a specific assessment tool from database with date-based session numbering"""
    conn = get_mysql_connection()
    cursor = conn.cursor()

    try:
        if tool_name == "Clients":
            cursor.execute("SELECT ID, counsellor_assn as 'Counsellor Assn', age as Age, gender as Gender, client_type as 'Client Type', county FROM clients")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[
                "ID",
                "Counsellor Assn",
                "Age",
                "Gender",
                "Client Type",
                "county",
            ])

        elif "EPDS" in tool_name:
            cursor.execute(
                """
                SELECT 
                    timestamp as Timestamp, 
                    client_code as 'Client Code', 
                    epds_total_score as 'EPDS Total Score (Max 30)', 
                    severity_descriptor as 'Severity Descriptor', 
                    item_10_raw_score as 'Item 10 (Harming Self) Raw Score', 
                    suicidality_flag as 'Suicidality Flag (Clinical Alert)', 
                    column_1 as 'Column 1',
                    ROW_NUMBER() OVER (PARTITION BY client_code ORDER BY timestamp) as 'Session Number'
                FROM epds_responses 
                ORDER BY client_code, timestamp
            """
            )
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[
                "Timestamp",
                "Client Code",
                "EPDS Total Score (Max 30)",
                "Severity Descriptor",
                "Item 10 (Harming Self) Raw Score",
                "Suicidality Flag (Clinical Alert)",
                "Column 1",
                "Session Number",
            ])

        elif "BDI" in tool_name:
            cursor.execute(
                """
                SELECT 
                    timestamp as Timestamp, 
                    client_code as 'Client Code', 
                    bdi_total as 'BDI Total', 
                    severity_level as 'Severity Level', 
                    clinical_interpretation as 'Clinical Interpretation',
                    ROW_NUMBER() OVER (PARTITION BY client_code ORDER BY timestamp) as 'Session Number'
                FROM bdi_responses 
                ORDER BY client_code, timestamp
            """
            )
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[
                "Timestamp",
                "Client Code",
                "BDI Total",
                "Severity Level",
                "Clinical Interpretation",
                "Session Number",
            ])

        elif "BAI" in tool_name:
            cursor.execute(
                """
                SELECT 
                    timestamp as Timestamp, 
                    client_code as 'Client Code', 
                    total_score as 'Total Score', 
                    severity as Severity, 
                    clinical_conclusion as 'Clinical Conclusion ',
                    ROW_NUMBER() OVER (PARTITION BY client_code ORDER BY timestamp) as 'Session Number'
                FROM bai_responses 
                ORDER BY client_code, timestamp
            """
            )
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[
                "Timestamp",
                "Client Code",
                "Total Score",
                "Severity",
                "Clinical Conclusion ",
                "Session Number",
            ])

        elif "ACE-Q" in tool_name:
            cursor.execute(
                """
                SELECT 
                    timestamp as Timestamp, 
                    client_code as 'Client Code', 
                    total_ace_score as 'Total ACE Score',
                    ROW_NUMBER() OVER (PARTITION BY client_code ORDER BY timestamp) as 'Session Number'
                FROM aceq_responses 
                ORDER BY client_code, timestamp
            """
            )
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[
                "Timestamp",
                "Client Code",
                "Total ACE Score",
                "Session Number",
            ])

        elif "SADS" in tool_name:
            cursor.execute(
                """
                SELECT 
                    timestamp as Timestamp, 
                    client_code as 'Client Code', 
                    social_avoidance_score as 'Social Avoidance Score', 
                    social_avoidance_level as 'Social Avoidance Level', 
                    social_distress_score as 'Social Distress Score', 
                    social_distress_level as 'Social Distress Level', 
                    total_sads_score as 'Total SADS Score', 
                    overall_level as 'Overall Level',
                    ROW_NUMBER() OVER (PARTITION BY client_code ORDER BY timestamp) as 'Session Number'
                FROM sads_responses 
                ORDER BY client_code, timestamp
            """
            )
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[
                "Timestamp",
                "Client Code",
                "Social Avoidance Score",
                "Social Avoidance Level",
                "Social Distress Score",
                "Social Distress Level",
                "Total SADS Score",
                "Overall Level",
                "Session Number",
            ])

        elif "ASRS" in tool_name:
            cursor.execute(
                """
                SELECT 
                    timestamp as Timestamp, 
                    client_code as 'Client Code', 
                    part_a_score as 'Part A Score', 
                    part_a_descriptor as 'Part A Descriptor', 
                    part_b_score as 'Part B Score', 
                    part_b_descriptor as 'Part B Descriptor', 
                    total_score as 'Total Score', 
                    total_descriptor as 'Total Descriptor', 
                    inattentive_subscale_raw as 'Inattentive Subscale (Raw)', 
                    inattentive_subscale_percent as 'Inattentive Subscale (%)', 
                    hyperactivity_motor_subscale_raw as 'Hyperactivity-Motor Subscale (Raw)', 
                    hyperactivity_motor_subscale_percent as 'Hyperactivity-Motor Subscale (%)', 
                    hyperactivity_verbal_subscale_raw as 'Hyperactivity-Verbal Subscale (Raw)', 
                    hyperactivity_verbal_subscale_percent as 'Hyperactivity-Verbal Subscale (%)',
                    ROW_NUMBER() OVER (PARTITION BY client_code ORDER BY timestamp) as 'Session Number'
                FROM asrs_responses 
                ORDER BY client_code, timestamp
            """
            )
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[
                "Timestamp",
                "Client Code",
                "Part A Score",
                "Part A Descriptor",
                "Part B Score",
                "Part B Descriptor",
                "Total Score",
                "Total Descriptor",
                "Inattentive Subscale (Raw)",
                "Inattentive Subscale (%)",
                "Hyperactivity-Motor Subscale (Raw)",
                "Hyperactivity-Motor Subscale (%)",
                "Hyperactivity-Verbal Subscale (Raw)",
                "Hyperactivity-Verbal Subscale (%)",
                "Session Number",
            ])

        else:
            df = pd.DataFrame()

        # Convert timestamp to datetime if present
        if not df.empty and "Timestamp" in df.columns:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    except Exception as e:
        st.error(f"Error loading {tool_name}: {e}")
        df = pd.DataFrame()
    finally:
        cursor.close()
        conn.close()
    return df


# Get client data from database
therapist_client_counts = get_therapist_client_counts()
clients_filtered = get_clients_filtered()


# Display comprehensive tool breakdown
with st.expander("View detailed client counts per assessment tool"):
    comprehensive_data = get_therapist_comprehensive_counts()
    
    if comprehensive_data:
        # Create a DataFrame for better display
        df_comprehensive = pd.DataFrame(comprehensive_data, columns=[
            "Therapist", "Total Clients", "EPDS", "BDI", "BAI", "ACE-Q", "SADS", "ASRS"
        ])
        
        # Display as an interactive table
        st.dataframe(df_comprehensive, width="stretch")
        
        # Summary statistics
        col1, col2 = st.columns(2)
        col3, col4 = st.columns(2)
        with col1:
            total_therapists = len(df_comprehensive)
            st.metric("Total Therapists", total_therapists)
        with col2:
            total_clients = df_comprehensive["Total Clients"].sum()
            st.metric("Total Clients", total_clients)
        with col3:
            avg_clients_per_therapist = df_comprehensive["Total Clients"].mean()
            st.metric("Avg Clients/Therapist", f"{avg_clients_per_therapist:.1f}")
        with col4:
            total_assessments = (df_comprehensive["EPDS"].sum() + 
                            df_comprehensive["BDI"].sum() + 
                            df_comprehensive["BAI"].sum() + 
                            df_comprehensive["ACE-Q"].sum() + 
                            df_comprehensive["SADS"].sum() + 
                            df_comprehensive["ASRS"].sum())
            st.metric("Total Tool Uses", total_assessments)
    else:
        st.info("No comprehensive data available yet.")

with st.expander("Available tools"):
    st.write(available_tools)

# st.header("📋 Assessment Tool Details")


with bottom():
    coll1, col2, col3 = st.columns([1, 2, 1])
    # Global therapist selection
    with col2:
        selected_therapist = "All"
        if therapist_client_counts:
            all_therapists = [therapist for therapist, count in therapist_client_counts]
            selected_therapist = st.selectbox(
                "🧑⚕️ Select Therapist (applies to all tools)",
                options=["All"] + all_therapists,
                key="global_therapist_selection",
            )


# Show all tool visuals in their own containers (not in tabs)
if available_tools:
    for tool_name in available_tools:
        with st.container():
            st.divider()
            st.subheader(f"🛠️ {tool_name} Data")
            # Load data from database (works for both cached and fresh data)
            df = get_tool_data(tool_name)

            if df.empty:
                st.warning(f"No data available for {tool_name}")
                continue

            st.caption(f"Available columns: {', '.join(df.columns.tolist())}")

            # Get therapist info from the db
            if "Client Code" in df.columns and "Counsellor Assn" not in df.columns:
                client_to_therapist = get_client_to_therapist_mapping()
                df["Counsellor Assn"] = df["Client Code"].map(client_to_therapist)
                therapist_col = "Counsellor Assn"
            else:
                therapist_col = (
                    "Counsellor Assn" if "Counsellor Assn" in df.columns else None
                )

            if therapist_col:
                if selected_therapist == "All":
                    filtered_df = df
                else:
                    filtered_df = df[df[therapist_col] == selected_therapist]

                st.subheader(f"{tool_name} - {selected_therapist}")

                score_cols = filtered_df.select_dtypes(
                    include=["number"]
                ).columns.tolist()

                if score_cols:
                    if "Client Code" in filtered_df.columns:
                        client_col = "Client Code"
                    elif "Client" in filtered_df.columns:
                        client_col = "Client"
                    elif "ID" in filtered_df.columns:
                        client_col = "ID"
                    else:
                        client_col = None

                if "Client Code" in filtered_df.columns:
                    client_col = "Client Code"
                elif "Client" in filtered_df.columns:
                    client_col = "Client"
                elif "ID" in filtered_df.columns:
                    client_col = "ID"
                else:
                    client_col = None

                if client_col and "Session Number" in filtered_df.columns:
                    viz_df = filtered_df.copy()

                    if score_cols:
                        selected_score = st.selectbox(
                            "Select score to visualize",
                            score_cols,
                            key=f"score_viz_{tool_name}_{selected_therapist}",
                        )

                        if not viz_df.empty:
                            # Create interactive Plotly figure
                            fig = go.Figure()

                            severity_ranges = get_severity_ranges(tool_name)

                            if severity_ranges:
                                max_session = viz_df["Session Number"].max()
                                for severity_name, (
                                    min_val,
                                    max_val,
                                    color,
                                ) in severity_ranges.items():
                                    fig.add_shape(
                                        type="rect",
                                        x0=0.5,
                                        x1=max_session + 0.5,
                                        y0=min_val,
                                        y1=max_val,
                                        fillcolor=color,
                                        opacity=0.2,
                                        layer="below",
                                        line_width=0,
                                    )
                                    fig.add_annotation(
                                        x=max_session + 0.7,
                                        y=(min_val + max_val) / 2,
                                        text=severity_name,
                                        showarrow=False,
                                        font=dict(size=10, color="black"),
                                        bgcolor="white",
                                        bordercolor="gray",
                                        borderwidth=1,
                                    )

                            colors = px.colors.qualitative.Set3
                            for i, client in enumerate(viz_df[client_col].unique()):
                                client_data = viz_df[
                                    viz_df[client_col] == client
                                ].sort_values("Session Number")
                                if not client_data.empty:
                                    fig.add_trace(
                                        go.Scatter(
                                            x=client_data["Session Number"],
                                            y=client_data[selected_score],
                                            mode="lines+markers",
                                            name=str(client),
                                            line=dict(
                                                width=2, color=colors[i % len(colors)]
                                            ),
                                            marker=dict(size=8),
                                            hovertemplate="<b>Client:</b> "
                                            + str(client)
                                            + "<br>"
                                            + "<b>Session:</b> %{x}<br>"
                                            + "<b>"
                                            + selected_score
                                            + ":</b> %{y}<br>"
                                            + "<extra></extra>",
                                        )
                                    )

                            # Calculate and plot average trajectory (line style to be updated next)
                            if len(viz_df[client_col].unique()) > 1:
                                avg_trajectory = (
                                    viz_df.groupby("Session Number")[selected_score]
                                    .mean()
                                    .reset_index()
                                )
                                if len(avg_trajectory) > 1:
                                    fig.add_trace(
                                        go.Scatter(
                                            x=avg_trajectory["Session Number"],
                                            y=avg_trajectory[selected_score],
                                            mode="lines+markers",
                                            name="Average",
                                            line=dict(width=4, color="grey", dash="dot"),
                                            marker=dict(size=12, color="grey"),
                                            hovertemplate="<b>Average</b><br>"
                                            + "<b>Session:</b> %{x}<br>"
                                            + "<b>"
                                            + selected_score
                                            + ":</b> %{y:.1f}<br>"
                                            + "<extra></extra>",
                                        )
                                    )

                            fig.update_layout(
                                title=f"{selected_score} Trajectories - {selected_therapist}",
                                xaxis_title="Session Number",
                                yaxis_title=f"{selected_score} Score",
                                hovermode="closest",
                                height=800,
                                showlegend=len(viz_df[client_col].unique()) <= 15,
                                legend=dict(
                                    orientation="v",
                                    yanchor="top",
                                    y=1,
                                    xanchor="left",
                                    x=1.02,
                                ),
                            )

                            fig.update_xaxes(
                                dtick=1,
                                range=[0.5, viz_df["Session Number"].max() + 0.5],
                            )

                            st.plotly_chart(fig, width="stretch")

                            if len(viz_df[client_col].unique()) > 1:
                                avg_trajectory = (
                                    viz_df.groupby("Session Number")[selected_score]
                                    .mean()
                                    .reset_index()
                                )
                                if len(avg_trajectory) > 1:
                                    improvement = (
                                        avg_trajectory.iloc[-1][selected_score]
                                        - avg_trajectory.iloc[0][selected_score]
                                    )
                                    improvement_pct = (
                                        (
                                            improvement
                                            / avg_trajectory.iloc[0][selected_score]
                                        )
                                        * 100
                                        if avg_trajectory.iloc[0][selected_score] != 0
                                        else 0
                                    )

                    else:
                        st.info("No numeric score columns available for visualization.")
                else:
                    st.info(
                        "Session data with proper numbering not available for trajectory analysis."
                    )

                st.subheader("Client Data")
                st.dataframe(filtered_df, width="content")
            else:
                st.info("No therapist column found. Displaying all data:")
                st.dataframe(df, width="content")

# Debug: Show all tabs in expander
with st.expander("🔍 View All Raw Data", False):
    for tool_name in available_tools:
        st.subheader(f"📄 {tool_name}")
        df = get_tool_data(tool_name)
        st.dataframe(df)
