"""
summary.py
Renders the top-level Summary Section in the Therapist Dashboard.

Usage (from app.py):
    from summary import render_summary
    render_summary(all_sheets)
"""

import streamlit as st
from sheets_data import get_global_summary


def render_summary(all_sheets: dict) -> None:
    """Render the full summary section at the top of the dashboard."""

    summary = get_global_summary(all_sheets)
    global_count = summary["global_unique_clients"]
    tools = summary["tools"]

    # ── Global headline ──────────────────────────────────────────────────────
    st.markdown("## 📊 Summary")
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #1e3a5f 0%, #2d6a9f 100%);
            border-radius: 12px;
            padding: 20px 28px;
            margin-bottom: 8px;
        ">
            <p style="color:#a8d4f5; font-size:13px; margin:0 0 4px 0; letter-spacing:1px; text-transform:uppercase;">
                Total Unique Clients — All Tools
            </p>
            <p style="color:#ffffff; font-size:48px; font-weight:700; margin:0; line-height:1.1;">
                {global_count}
            </p>
            <p style="color:#7ec8e3; font-size:13px; margin:6px 0 0 0;">
                Distinct client IDs seen across every assessment tool sheet
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Per-tool sections ─────────────────────────────────────────────────────
    for i, tool in enumerate(tools):
        # Section header with full tool name
        st.markdown(
            f"""
            <div style="
                border-left: 5px solid #2d6a9f;
                padding: 6px 0 6px 14px;
                margin: 4px 0 10px 0;
            ">
                <span style="font-size:18px; font-weight:600;">{tool['full_name']}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Four metric cards
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.metric(
                label="Clients Assessed",
                value=tool["total_clients"],
                help="Number of distinct client IDs that have submitted this tool",
            )

        with c2:
            st.metric(
                label="Clients with only one assessment point",
                value=tool["one_timepoint"],
                help="Clients with exactly one submission — more assessments expected",
            )

        with c3:
            st.metric(
                label="Clients with multiple assessment points",
                value=tool["multi_assessment"],
                help="Clients with more than one submission",
            )

        with c4:
            if tool["avg_change_pct"] is not None:
                value_str = f"{tool['avg_change_pct']:+.1f}%"
                # Negative = improvement for symptom scales, positive = worsening
                delta_colour = "inverse"   # green when negative, red when positive
                st.metric(
                    label="Average Score Change",
                    value=value_str,
                    help=(
                        "Average percentage change in score from first to last assessment "
                        "for clients with ≥ 2 data points. "
                        "Negative = score decreased (typically improvement)."
                    ),
                )
            else:
                st.metric(
                    label="Avg Score Change (first→last)",
                    value="N/A",
                    help="Not enough multi-point data to compute a percentage change",
                )

        # Divider between tools (skip after the last one)
        if i < len(tools) - 1:
            st.divider()
