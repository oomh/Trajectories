# Therapist Dashboard

A simple dashboard for Mental 360 therapists to view their clients' score trajectories on standardised mental health outcome measures.

## Features

- View client progress across multiple assessment tools (EPDS, BDI, BAI, ACE-Q, SADS, ASRS)
- Interactive trajectory visualizations with severity range shading
- Therapist-specific filtering
- Session-by-session progress tracking

## Assessment Tools

- **EPDS** - Edinburgh Postnatal Depression Scale
- **BDI** - Beck Depression Inventory
- **BAI** - Beck Anxiety Inventory  
- **ACE-Q** - Adverse Childhood Experiences Questionnaire
- **SADS** - Social Avoidance and Distress Scale
- **ASRS** - Adult ADHD Self-Report Scale

## MySQL Server Setup (for deployment)

To run this app, you need a local or remote MySQL server. Here’s how to set up a local MySQL server (example for Ubuntu/Debian/Fedora):

### 1. Install MySQL Server

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install mysql-server
```

**Fedora:**

```bash
sudo dnf install @mysql
sudo systemctl start mysqld
```

### 2. Secure MySQL (optional but recommended)

```bash
sudo mysql_secure_installation
```

### 3. Create Database and User

Log in as root (use `sudo mysql -u root` if you don’t know the password):

```bash
sudo mysql -u root
```

Then run:

```sql
CREATE DATABASE therapist_dashboard;
CREATE USER 'therapist_user'@'localhost' IDENTIFIED BY 'therapist_pass';
GRANT ALL PRIVILEGES ON therapist_dashboard.* TO 'therapist_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

### 4. Update Credentials

Edit `db_build.py` and set the `MYSQL_CONFIG` dictionary to match:

```toml
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'therapist_user',
    'password': 'therapist_pass',
    'database': 'therapist_dashboard'
}
```

### 5. Populate the Database

Run:

```bash
python populate_mysql.py
```

if you want to do it manually, or simply run:

```bash
streamlit run app.py
```

the app will build the DB on first run.

This will create all tables and load data from Google Sheets.

### 6. Run the App

```bash
streamlit run app.py
```

## Usage

1. Select a therapist from the dropdown (or view all)
2. Navigate between assessment tool tabs
3. Choose which score to visualize
4. View client trajectories with severity indicators
