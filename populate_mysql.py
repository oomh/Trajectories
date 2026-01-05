"""
populate_mysql.py
Populate the local MySQL database for the Therapist Dashboard app.
Run this after setup_mysql_local.sh and updating db_build.py with correct credentials.
"""

from db_build import init_database, full_data_refresh

def main():
    print("Initializing MySQL schema...")
    init_database()
    print("Populating database from Google Sheets...")
    full_data_refresh()
    print("Database population complete.")

if __name__ == "__main__":
    main()