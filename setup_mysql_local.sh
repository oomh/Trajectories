#!/bin/bash
# setup_mysql_local.sh
# This script sets up a local MySQL server, creates the required database, user, and grants privileges for the Therapist Dashboard app.
# Supports Ubuntu/Debian, Fedora/RHEL/CentOS, macOS, and Windows (Git Bash/WSL)

# --- CONFIGURABLE VARIABLES ---
MYSQL_ROOT_PASSWORD="rootpassword"   # Change as needed
MYSQL_USER="therapist_user"
MYSQL_PASSWORD="therapist_pass"
MYSQL_DATABASE="therapist_dashboard"

# --- OS DETECTION AND SELECTION ---
echo "========================================"
echo "MySQL Server Setup for Therapist Dashboard"
echo "========================================"
echo ""
echo "Please select your operating system:"
echo "1) Ubuntu/Debian"
echo "2) Fedora/RHEL/CentOS"
echo "3) macOS"
echo "4) Windows"
echo "5) Auto-detect"
echo ""
read -p "Enter your choice [1-5]: " os_choice

# Auto-detect OS if user chooses option 5
if [ "$os_choice" = "5" ]; then
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if [ -f /etc/debian_version ]; then
            os_choice="1"
            echo "Detected: Ubuntu/Debian"
        elif [ -f /etc/fedora-release ] || [ -f /etc/redhat-release ]; then
            os_choice="2"
            echo "Detected: Fedora/RHEL/CentOS"
        else
            echo "Unable to auto-detect Linux distribution. Please select manually."
            exit 1
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        os_choice="3"
        echo "Detected: macOS"
    elif [[ "$OSTYPE" == "msys"* || "$OSTYPE" == "cygwin"* || "$OSTYPE" == "win32"* ]]; then
        os_choice="4"
        echo "Detected: Windows"
    else
        echo "Unable to auto-detect OS. Please select manually."
        exit 1
    fi
fi

echo ""
echo "Installing MySQL server..."

# --- INSTALL MYSQL SERVER BASED ON OS ---
case $os_choice in
    1)
        # Ubuntu/Debian
        echo "Installing for Ubuntu/Debian..."
        sudo apt-get update
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server
        
        echo "Starting MySQL service..."
        sudo service mysql start
        
        # Enable MySQL to start on boot
        sudo systemctl enable mysql
        ;;
    
    2)
        # Fedora/RHEL/CentOS
        echo "Installing for Fedora/RHEL/CentOS..."
        sudo dnf install -y @mysql || sudo yum install -y mysql-server
        
        echo "Starting MySQL service..."
        sudo systemctl start mysqld
        
        # Enable MySQL to start on boot
        sudo systemctl enable mysqld
        
        # Get temporary root password for MySQL 8.0+
        if [ -f /var/log/mysqld.log ]; then
            TEMP_PASSWORD=$(sudo grep 'temporary password' /var/log/mysqld.log | awk '{print $NF}' | tail -1)
            if [ -n "$TEMP_PASSWORD" ]; then
                echo "Temporary MySQL root password found: $TEMP_PASSWORD"
                echo "You may need to change it during initial login."
            fi
        fi
        ;;
    
    3)
        # macOS
        echo "Installing for macOS..."
        
        # Check if Homebrew is installed
        if ! command -v brew &> /dev/null; then
            echo "Homebrew not found. Installing Homebrew..."
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        fi
        
        echo "Installing MySQL via Homebrew..."
        brew install mysql
        
        echo "Starting MySQL service..."
        brew services start mysql
        ;;

    4)
        # Windows (Git Bash/WSL)
        echo "Installing for Windows..."
        echo "Note: This script expects Git Bash or WSL." 
        
        if command -v winget &> /dev/null; then
            echo "Installing MySQL via winget..."
            winget install -e --id Oracle.MySQL
        elif command -v choco &> /dev/null; then
            echo "Installing MySQL via Chocolatey..."
            choco install -y mysql
        else
            echo "Neither winget nor Chocolatey found."
            echo "Please install MySQL manually from: https://dev.mysql.com/downloads/"
        fi

        echo "Attempting to start MySQL service..."
        if command -v powershell.exe &> /dev/null; then
            powershell.exe -NoProfile -Command "Start-Service -Name MySQL80 -ErrorAction SilentlyContinue; Start-Service -Name MySQL -ErrorAction SilentlyContinue"
        else
            echo "PowerShell not found. Please start the MySQL service manually."
        fi
        ;;
    
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac

echo ""
echo "MySQL server installed and started successfully."
echo ""

# --- SECURE MYSQL INSTALLATION (optional) ---
echo "Note: You may want to run 'sudo mysql_secure_installation' for additional security."
echo ""

# --- CREATE DATABASE AND USER ---
echo "Creating database and user..."

# Different commands for different OS
if [ "$os_choice" = "3" ]; then
    # macOS - MySQL installed via Homebrew usually doesn't require sudo
    mysql -u root <<EOF
CREATE DATABASE IF NOT EXISTS $MYSQL_DATABASE;
CREATE USER IF NOT EXISTS '$MYSQL_USER'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD';
GRANT ALL PRIVILEGES ON $MYSQL_DATABASE.* TO '$MYSQL_USER'@'localhost';
FLUSH PRIVILEGES;
EOF
elif [ "$os_choice" = "4" ]; then
    # Windows - MySQL root password is set during installer
    if [ -z "$MYSQL_ROOT_PASSWORD" ]; then
        read -s -p "Enter MySQL root password: " MYSQL_ROOT_PASSWORD
        echo ""
    fi
    mysql -u root -p"$MYSQL_ROOT_PASSWORD" <<EOF
CREATE DATABASE IF NOT EXISTS $MYSQL_DATABASE;
CREATE USER IF NOT EXISTS '$MYSQL_USER'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD';
GRANT ALL PRIVILEGES ON $MYSQL_DATABASE.* TO '$MYSQL_USER'@'localhost';
FLUSH PRIVILEGES;
EOF
else
    # Linux - Try with sudo first
    sudo mysql -u root <<EOF
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '$MYSQL_ROOT_PASSWORD';
FLUSH PRIVILEGES;
CREATE DATABASE IF NOT EXISTS $MYSQL_DATABASE;
CREATE USER IF NOT EXISTS '$MYSQL_USER'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD';
GRANT ALL PRIVILEGES ON $MYSQL_DATABASE.* TO '$MYSQL_USER'@'localhost';
FLUSH PRIVILEGES;
EOF
fi

echo ""
echo "========================================"
echo "MySQL local server setup complete!"
echo "========================================"
echo "Database: $MYSQL_DATABASE"
echo "User: $MYSQL_USER"
echo "Password: $MYSQL_PASSWORD"
echo ""
echo "Next steps:"
echo "1. Update your db_build.py MYSQL_CONFIG to use these credentials"
echo "2. Run: python populate_mysql.py"
echo "3. Run: streamlit run app.py"
echo "========================================"
