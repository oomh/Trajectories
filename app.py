"""
app.py - Therapist Dashboard
Cloud-ready version (no database required)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sheets_data import (
    get_all_sheets_data,
    get_available_tools,
    get_therapist_list,
    get_clients_for_therapist,
    get_therapist_comprehensive_counts
)

st.set_page_config(page_title="Therapist Dashboard", layout="wide")

# Header with refresh button
col1, col2 = st.columns([0.9, 0.1])
with col1:
    st.title("🩺 Therapist Dashboard")
    st.markdown("View client progress across assessment tools")
    if st.button("🔄 Refresh", key="refresh_button", help="Refresh data from Google Sheets"):
        st.cache_data.clear()
        st.rerun()

# Load data
with st.spinner("Loading data from Google Sheets..."):
    all_sheets = get_all_sheets_data()
    st.success("Data loaded successfully!")

clients_df = all_sheets.get("Clients", pd.DataFrame())
available_tools = get_available_tools(all_sheets)

# Display response count cards in a grid
st.subheader("Assessment Response Counts")
cols = st.columns(min(3, len(available_tools)))

for idx, tool_name in enumerate(available_tools):
    tool_df = all_sheets.get(tool_name, pd.DataFrame())
    response_count = len(tool_df)
    
    # Extract short tool name
    if 'EPDS' in tool_name:
        short_name = "EPDS"
    elif 'BDI' in tool_name:
        short_name = "BDI"
    elif 'BAI' in tool_name:
        short_name = "BAI"
    elif 'ACE-Q' in tool_name:
        short_name = "ACE-Q"
    elif 'SADS' in tool_name:
        short_name = "SADS"
    elif 'ASRS' in tool_name:
        short_name = "ASRS"
    else:
        short_name = tool_name[:10]
    
    with cols[idx % len(cols)]:
        st.metric(
            short_name,
            response_count,
            delta=None,
            help=tool_name
        )

st.divider()


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


# Display comprehensive tool breakdown
with st.expander("View detailed client counts per assessment tool"):
    comprehensive_data = get_therapist_comprehensive_counts(all_sheets)
    
    if comprehensive_data:
        # Create display data
        display_data = []
        for item in comprehensive_data:
            row = {
                "Therapist": item['therapist'],
                "Total Clients": item['total_clients'],
            }
            for tool, count in item['tool_counts'].items():
                # Extract tool short name
                if 'EPDS' in tool:
                    row['EPDS'] = count
                elif 'BDI' in tool:
                    row['BDI'] = count
                elif 'BAI' in tool:
                    row['BAI'] = count
                elif 'ACE-Q' in tool:
                    row['ACE-Q'] = count
                elif 'SADS' in tool:
                    row['SADS'] = count
                elif 'ASRS' in tool:
                    row['ASRS'] = count
            display_data.append(row)
        
        df_comprehensive = pd.DataFrame(display_data)
        st.dataframe(df_comprehensive, width="stretch")
        
        # Summary statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Therapists", len(df_comprehensive))
        with col2:
            st.metric("Total Clients", df_comprehensive["Total Clients"].sum())
        with col3:
            st.metric("Avg Clients/Therapist", f"{df_comprehensive['Total Clients'].mean():.1f}")
        with col4:
            # Count total assessments
            tool_cols = ['EPDS', 'BDI', 'BAI', 'ACE-Q', 'SADS', 'ASRS']
            total_assessments = sum(df_comprehensive.get(col, pd.Series([0])).sum() for col in tool_cols)
            st.metric("Total Assessments", int(total_assessments))
    else:
        st.info("No comprehensive data available yet.")

# Global therapist selection
therapist_list = get_therapist_list(clients_df)
selected_therapist = st.selectbox(
    "Select Therapist (applies to all tools)",
    options=["All"] + therapist_list,
    key="global_therapist_selection",
)

# Option to focus on specific client
st.write("---")
focus_on_client = st.checkbox("Focus on specific client trajectory", value=False)
selected_client_focus = None
if focus_on_client:
    filtered_clients = get_clients_for_therapist(clients_df, selected_therapist)
    client_list = filtered_clients['ID'].dropna().unique().tolist()
    selected_client_focus = st.selectbox(
        "Select Client",
        options=client_list,
        key="client_focus_selection",
    )

# Show all tool visuals in tabs
if available_tools:
    tabs = st.tabs([f"{tool.split('(')[0].strip()[:20]}" for tool in available_tools])
    
    for idx, tool_name in enumerate(available_tools):
        with tabs[idx]:
            st.subheader(tool_name)
            
            # Get tool data from sheets
            tool_df = all_sheets.get(tool_name, pd.DataFrame())
            
            if tool_df.empty:
                st.warning(f"No data available for {tool_name}")
                continue
            
            # Filter by therapist if needed
            if "Client Code" in tool_df.columns and selected_therapist != "All":
                client_ids_for_therapist = clients_df[
                    clients_df['Counsellor Assn`'] == selected_therapist
                ]['ID'].unique()
                tool_df = tool_df[tool_df['Client Code'].isin(client_ids_for_therapist)]
            
            if tool_df.empty:
                st.info(f"No data for {selected_therapist} on this tool")
                continue
            
            # Find score column
            score_col = None
            if 'EPDS' in tool_name and 'EPDS Total Score (Max 30)' in tool_df.columns:
                score_col = 'EPDS Total Score (Max 30)'
            elif 'BDI' in tool_name and 'BDI Total' in tool_df.columns:
                score_col = 'BDI Total'
            elif 'BAI' in tool_name and 'Total Score' in tool_df.columns:
                score_col = 'Total Score'
            elif 'ACE-Q' in tool_name and 'Total ACE Score' in tool_df.columns:
                score_col = 'Total ACE Score'
            elif 'SADS' in tool_name and 'Total SADS Score' in tool_df.columns:
                score_col = 'Total SADS Score'
            elif 'ASRS' in tool_name and 'Total Score' in tool_df.columns:
                score_col = 'Total Score'
            
            if score_col and score_col in tool_df.columns:
                # Clean data
                viz_df = tool_df.copy()
                viz_df[score_col] = pd.to_numeric(viz_df[score_col], errors='coerce')
                viz_df = viz_df.dropna(subset=[score_col, 'Client Code'])
                
                # Filter by client if focus mode is on
                if focus_on_client and selected_client_focus:
                    viz_df = viz_df[viz_df['Client Code'] == selected_client_focus]
                
                if viz_df.empty:
                    st.info("No data for selected filter")
                    continue
                
                # Add session numbers
                viz_df['Session'] = viz_df.groupby('Client Code').cumcount() + 1
                
                # Create Plotly figure
                fig = go.Figure()
                
                # Add severity range backgrounds
                severity_ranges = get_severity_ranges(tool_name)
                if severity_ranges:
                    max_session = viz_df['Session'].max()
                    for severity_name, (min_val, max_val, color) in severity_ranges.items():
                        fig.add_shape(
                            type="rect",
                            x0=0.5,
                            x1=max_session + 0.5,
                            y0=min_val,
                            y1=max_val,
                            fillcolor=color,
                            opacity=0.15,
                            layer="below",
                            line_width=0,
                        )
                        # Add label
                        fig.add_annotation(
                            x=max_session + 0.7,
                            y=(min_val + max_val) / 2,
                            text=severity_name,
                            showarrow=False,
                            font=dict(size=9, color="black"),
                            bgcolor="rgba(255,255,255,0.8)",
                            bordercolor="gray",
                            borderwidth=1,
                        )
                
                # Plot individual client trajectories
                colors = px.colors.qualitative.Set3
                clients = viz_df['Client Code'].unique()
                
                for i, client in enumerate(clients):
                    client_data = viz_df[viz_df['Client Code'] == client].sort_values('Session')
                    if not client_data.empty:
                        fig.add_trace(
                            go.Scatter(
                                x=client_data['Session'],
                                y=client_data[score_col],
                                mode='lines+markers',
                                name=str(client),
                                line=dict(width=2, color=colors[i % len(colors)]),
                                marker=dict(size=8),
                                hovertemplate=f"<b>Client:</b> {client}<br><b>Session:</b> %{{x}}<br><b>{score_col}:</b> %{{y}}<extra></extra>",
                            )
                        )
                
                # Add average trajectory if multiple clients
                if len(clients) > 1:
                    avg_trajectory = viz_df.groupby('Session')[score_col].mean().reset_index()
                    fig.add_trace(
                        go.Scatter(
                            x=avg_trajectory['Session'],
                            y=avg_trajectory[score_col],
                            mode='lines+markers',
                            name='Average',
                            line=dict(width=4, color='grey', dash='dot'),
                            marker=dict(size=12, color='grey'),
                            hovertemplate=f"<b>Average</b><br><b>Session:</b> %{{x}}<br><b>{score_col}:</b> %{{y:.1f}}<extra></extra>",
                        )
                    )
                
                fig.update_layout(
                    title=f"{score_col} Trajectories - {selected_therapist}" if not focus_on_client else f"{score_col} - Client {selected_client_focus}",
                    xaxis_title="Session Number",
                    yaxis_title=f"{score_col} Score",
                    hovermode='closest',
                    height=700,
                    showlegend=True,
                    legend=dict(
                        orientation="v",
                        yanchor="top",
                        y=0.99,
                        xanchor="left",
                        x=1.02,
                    ),
                )
                
                fig.update_xaxes(dtick=1)
                st.plotly_chart(fig, width="stretch")
                
                # Display summary stats
                if len(clients) > 1:
                    avg_trajectory = viz_df.groupby('Session')[score_col].mean().reset_index()
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Number of Clients", len(clients))
                    with col2:
                        st.metric("Sessions Completed", viz_df['Session'].max())
                    with col3:
                        if len(avg_trajectory) > 1:
                            improvement = avg_trajectory.iloc[-1][score_col] - avg_trajectory.iloc[0][score_col]
                            st.metric("Avg Change", f"{improvement:.1f}")
            
            # Display raw data
            st.subheader("Raw Data")
            st.dataframe(tool_df, width="stretch")

else:
    st.info("No assessment tools found in data")

# Footer
st.divider()
st.markdown("**Data Source:** Google Sheets | **Cache:** Auto-refreshed hourly")
