# Therapist Dashboard

<https://trajectories.streamlit.app/>

A Streamlit dashboard for Mental 360 therapists to view their clients' score trajectories on standardised mental health outcome measures. Data is sourced directly from Google Sheets.

## Features

- View client progress across multiple assessment tools (EPDS, BDI, BAI, ACE-Q, SADS, ASRS)
- Interactive trajectory visualizations with severity range shading
- Therapist-specific filtering
- Session-by-session progress tracking
- Client focus mode for detailed individual trajectories
- Real-time data refresh from Google Sheets
- Response count overview cards for each assessment tool

## Assessment Tools

- **EPDS** - Edinburgh Postnatal Depression Scale
- **BDI** - Beck Depression Inventory
- **BAI** - Beck Anxiety Inventory  
- **ACE-Q** - Adverse Childhood Experiences Questionnaire
- **SADS** - Social Avoidance and Distress Scale
- **ASRS** - Adult ADHD Self-Report Scale

## Usage

1. Select a therapist from the dropdown (or view all therapists)
2. Optionally check "Focus on specific client trajectory" to view a single client's progress
3. Navigate between assessment tool tabs
4. Choose which score to visualize
5. View client trajectories with severity-based background shading
6. Click the refresh button to reload data from Google Sheets

## Architecture

- **app.py** - Main Streamlit dashboard application
- **sheets_data.py** - Google Sheets data layer with caching
- **sheets_pull.py** - Google Sheets API integration
- **requirements.txt** - Python dependencies
- **.streamlit/config.toml** - Streamlit configuration
