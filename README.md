# Store Database Management System

Enterprise-grade store management application with role-based authentication, real-time ETL operations, and comprehensive data analytics. Built with Python, PySide6, and MySQL for managing retail operations including inventory, orders, staff, and customer data.

**Key Capabilities:**
- 🔐 Multi-level user authentication (Employee/Manager/Administrator)
- 📊 Real-time database operations with progress tracking
- 📄 PDF report generation for customer analytics
- 🔄 ETL pipeline for CSV and API data integration
- 🎨 Professional desktop UI with light/dark themes
- 🛡️ Enterprise security features (account lockout, password policies, session management)

## Key Features

### Authentication & Security
- **Role-Based Access Control**: 3 distinct user levels with granular permissions
- **Two-Factor Authentication (2FA)**: Mandatory TOTP-based 2FA for administrators
- **bcrypt Password Hashing**: Industry-standard Argon2-based encryption
- **Session Management**: Singleton pattern with automatic timeout
- **Account Lockout**: Protection against brute-force attacks
- **Password Policy Enforcement**: Complexity requirements with validation
- **Audit Trail**: Login tracking and failed attempt monitoring
- **Backup Codes**: Single-use recovery codes for account access

### User Interface
- **Modern PySide6 (Qt6)**: Professional desktop application framework
- **Dual Theme System**: Light and dark themes with instant switching
- **Responsive Design**: Adaptive layouts for different screen sizes
- **Multi-threaded Operations**: Non-blocking UI during database operations
- **Real-time Progress Tracking**: Live progress bars and status updates
- **Tabbed Interface**: Organized workspace with role-based tab visibility

### Data Management
- **PDF Report Generation**: Customer data export with professional formatting (reportlab)
- **CSV Import/Export**: Bulk data operations with validation
- **API Integration**: RESTful API client with automatic endpoint discovery
- **Batch Processing**: Optimized insert/update/delete operations
- **Data Validation**: Schema alignment and integrity checks
- **NaN→NULL Conversion**: Proper handling of missing data for MySQL

### Database Operations
- **PyMySQL Driver**: Pure Python MySQL connector with connection pooling
- **10-Table Schema**: Comprehensive relational database with foreign keys
- **Transaction Support**: InnoDB engine with ACID compliance
- **Auto-schema Creation**: Database and tables created automatically
- **Migration Support**: Schema updates and version management
- **Query Optimization**: Indexed columns and efficient joins

### API & External Data
- **Smart Endpoint Detection**: Auto-discovery of API structure and routes
- **Multiple Server Support**: Works with different API architectures
- **Fallback Logic**: Automatic retry with alternative endpoints
- **Rate Limiting**: Configurable request throttling to prevent API abuse
- **Retry Handler**: Exponential backoff strategy for failed requests
- **Error Recovery**: Graceful handling of network and server errors

### Development Features
- **Modular Architecture**: Separation of concerns with clear interfaces
- **Refactored Codebase**: Dashboard and admin windows use UI builder + handler pattern (64% code reduction)
- **Facade Pattern**: Simplified interfaces for complex subsystems
- **Worker Thread Pattern**: Async operations with Qt signals/slots
- **Comprehensive Logging**: Structured JSON logs with correlation IDs
- **Exception System**: Hierarchical exception handling with context
- **Testing Suite**: Unit tests for critical components

## Requirements

### System Requirements
- **Python**: 3.11+ (recommended: 3.13.7)
- **MySQL Server**: 8.0+

### Python Dependencies
```
pandas>=2.0.0
pymysql>=1.0.0              # Primary MySQL driver (pure Python)
mysql-connector-python>=8.0  # Fallback MySQL driver (optional)
requests>=2.28.0
python-dotenv>=1.0.0
PySide6>=6.4.0
bcrypt>=4.0.0
pyotp>=2.9.0
qrcode>=7.4.2
reportlab>=4.0.0
psutil>=5.9.0
```

**Install:**
```bash
# Core dependencies (PyMySQL)
pip install pandas pymysql requests python-dotenv PySide6 bcrypt pyotp qrcode reportlab psutil

# Optional: mysql-connector-python (automatic fallback if PyMySQL unavailable)
pip install mysql-connector-python
```

**Note**: Application works with either PyMySQL or mysql-connector-python. PyMySQL is recommended for Python 3.13+ compatibility.

## Installation

### Prerequisites

1. **Python 3.11+** (Python 3.13.7 recommended)
   - Download from [python.org](https://www.python.org/downloads/)
   - During installation, check "Add Python to PATH"

2. **MySQL Server 8.0+**
   - Download from [mysql.com](https://dev.mysql.com/downloads/mysql/)
   - Remember your root password during installation
   - Ensure MySQL service is running

3. **Git** (optional, for cloning)
   - Download from [git-scm.com](https://git-scm.com/downloads/)

### Step-by-Step Installation

**1. Get the Project**
```bash
# Option A: Clone with Git
git clone https://github.com/Sylverose/Advanced-Python-ETL-Pipeline-with-GUI.git
cd Advanced-Python-ETL-Pipeline-with-GUI

# Option B: Download ZIP
# Download from GitHub → Extract → Open folder in terminal
```

**2. Create Virtual Environment (Recommended)**
```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/Mac
python3 -m venv .venv
source .venv/bin/activate
```

**3. Install Dependencies**
```bash
pip install -r requirements.txt
```

**4. Configure Database**

Create a `.env` file in the project root directory:
```bash
# Windows (PowerShell)
New-Item .env -ItemType File

# Linux/Mac
touch .env
```

Edit `.env` with your MySQL credentials:
```
DB_USER=root
DB_PASSWORD=your_mysql_password_here
DB_HOST=127.0.0.1
DB_NAME=store_manager
```

**5. Initialize Authentication System**
```bash
python initialize_auth.py
```

This will:
- Connect to MySQL and create the `store_manager` database
- Create the `users` table
- Prompt you to create an administrator account
- Optionally link the admin to an existing staff member

**6. Launch the Application**
```bash
python run_app.py
```

### Verification

After installation, verify everything works:

1. **Database Connection**: The app should connect to MySQL without errors
2. **Login Window**: You should see the login screen
3. **Admin Login**: Use the credentials you created in step 5
4. **Dashboard**: After login, you should see the dashboard

### Troubleshooting Installation

**"python is not recognized"**
- Python is not in your PATH
- Reinstall Python and check "Add Python to PATH"
- Or use full path: `C:\Python313\python.exe`

**"pip install fails"**
- Upgrade pip: `python -m pip install --upgrade pip`
- Use `pip3` instead of `pip` on Linux/Mac

**"Could not connect to database"**
- Ensure MySQL service is running
- Check your credentials in `.env`
- Test connection: `mysql -u root -p` in terminal

**"Access denied for user"**
- Verify password in `.env` matches MySQL root password
- Try resetting MySQL password

**"No module named 'PySide6'"**
- Virtual environment not activated
- Run: `.venv\Scripts\activate` (Windows) or `source .venv/bin/activate` (Linux/Mac)
- Reinstall: `pip install -r requirements.txt`

**"initialize_auth.py fails"**
- Ensure database is configured correctly in `.env`
- Check MySQL service is running
- Verify database permissions for the user

## Usage

### First-Time Usage

**1. Launch the Application**
```bash
python run_app.py
```

**2. Login**
- Enter the administrator username and password you created during `initialize_auth.py`
- Click "Login"

**3. Explore the Dashboard**

Based on your role, you'll see different options:

| Feature | Employee | Manager | Administrator |
|---------|----------|---------|---------------|
| View Dashboard | ✅ | ✅ | ✅ |
| View/Export Data | ✅ | ✅ | ✅ |
| Import/Modify Data | ❌ | ✅ | ✅ |
| View Users/Logs | ❌ | ✅ | ✅ |
| Manage Database | ❌ | ❌ | ✅ |
| Manage Users | ❌ | ❌ | ✅ |

**4. Administrator Actions**

As an administrator, you can:

- **Manage Database**: Click "Manage Database" to open the ETL admin window
  - Import CSV files
  - Fetch API data
  - Create/manage database tables
  - Run tests and validations

- **Manage Users**: Click "Manage Users" to:
  - Create new user accounts (Employee, Manager, Administrator)
  - Change user roles
  - Activate/deactivate accounts
  - Link users to staff members

- **Generate Reports**: Export customer data to PDF format

### Common Workflows

**Creating a New User (Admin Only)**
1. Login as Administrator
2. Click "Manage Users" button on dashboard
3. Go to "Create User" tab
4. Fill in: username, password, role, optional staff ID
5. Click "Create User"

**Loading Data (Admin Only)**
1. Click "Manage Database"
2. In the ETL window:
   - **For CSV**: Click "Select CSV Files", choose files, click "Load CSV to Database"
   - **For API**: Click "Fetch API Data to Database"
3. Monitor progress in the output section

**Exporting Reports**
1. In the dashboard, find the "Customer Reporting" section
2. Select customer from dropdown
3. Click "Generate PDF Report"
4. PDF saves to `data/print/` folder

### Alternative Launch Methods

```bash
# Production: Launch with authentication (recommended)
python run_app.py

# Development: Direct admin access (bypasses authentication)
python run_admin_direct.py

# CLI: Run ETL operations from command line
python src/main.py
```

**When to use `run_admin_direct.py`:**
- Development and testing
- Quick database operations without login
- Bypasses all authentication (use only in secure environments)

## Authentication System

### Overview

The application uses a comprehensive role-based authentication system with three user levels:

| Role | Description | Typical Use Case |
|------|-------------|------------------|
| **Employee** | Basic access to view and export data | Data analysts, viewers |
| **Manager** | Can import and modify data | Department managers, data managers |
| **Administrator** | Full system access including user management | System administrators, IT staff |

### User Roles & Permissions

| Feature | Employee | Manager | Administrator |
|---------|----------|---------|---------------|
| View dashboard & data | ✅ | ✅ | ✅ |
| Export data (CSV/PDF) | ✅ | ✅ | ✅ |
| Import CSV data | ❌ | ✅ | ✅ |
| Modify/update data | ❌ | ✅ | ✅ |
| View users & logs | ❌ | ✅ | ✅ |
| Delete data | ❌ | ❌ | ✅ |
| Manage database (full ETL) | ❌ | ❌ | ✅ |
| Create/manage users | ❌ | ❌ | ✅ |
| Change system settings | ❌ | ❌ | ✅ |

### Initial Setup

**Step 1: Run initialization script**
```bash
python initialize_auth.py
```

**Step 2: Follow the prompts**
```
Welcome to the ETL Pipeline Manager - Authentication Setup

Enter admin username: admin
Enter admin password: ********
Confirm password: ********

Link to existing staff member? (y/n): n

✅ Administrator account created successfully!
   Username: admin
   Role: Administrator
```

**Step 3: Login**
```bash
python run_app.py
```

Use the credentials you just created to login.

### Security Features

- ✅ **Two-Factor Authentication (2FA)**: TOTP-based authentication (mandatory for administrators)
- ✅ **bcrypt Password Hashing**: Military-grade password encryption
- ✅ **Account Lockout**: Automatic lockout after failed login attempts
- ✅ **Password Policy**: Enforces minimum complexity requirements
- ✅ **Session Management**: Secure session handling with timeout
- ✅ **Session Timeout**: Automatic logout after inactivity
- ✅ **Password Change Enforcement**: Can require password changes
- ✅ **Active Status**: Deactivate users without deleting accounts
- ✅ **Backup Codes**: 8 single-use recovery codes per user

### Users Table Schema

```sql
CREATE TABLE users (
    user_id INT PRIMARY KEY AUTO_INCREMENT,
    staff_id INT UNIQUE,                    -- Optional link to staff
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,    -- bcrypt hashed
    role ENUM('Employee', 'Manager', 'Administrator') NOT NULL,
    last_login DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT TRUE,
    failed_login_attempts INT DEFAULT 0,
    last_failed_login DATETIME,
    account_locked_until DATETIME,
    password_last_changed DATETIME DEFAULT CURRENT_TIMESTAMP,
    must_change_password BOOLEAN DEFAULT FALSE,
    two_factor_enabled BOOLEAN DEFAULT FALSE,
    two_factor_secret VARCHAR(32),
    backup_codes TEXT,                      -- JSON array of backup codes
    FOREIGN KEY (staff_id) REFERENCES staffs(staff_id)
)
```

### User Management (Admin Only)

**GUI Method:**
1. Login as Administrator
2. Click "Manage Users" button on dashboard
3. **Create Tab**: Add new users
   - Enter username (must be unique)
   - Set password (will be hashed automatically)
   - Select role (Employee/Manager/Administrator)
   - Optional: Link to staff member by ID
4. **Manage Tab**: View and manage existing users
   - Change user roles
   - Activate/deactivate accounts
   - View user details and last login

**Programmatic Method:**
```python
from auth.user_manager import UserManager
from connect import connect_to_mysql
from config.database import DatabaseConfig

# Setup
db_config = DatabaseConfig().to_dict()
connection = connect_to_mysql(db_config)
user_manager = UserManager(connection)

# Create a new user
user_manager.create_user(
    username="john.doe",
    password="SecurePass123!",
    role="Manager",
    staff_id=5  # Optional: links to staffs table
)

# Change user role
user_manager.update_user_role(user_id=2, new_role="Administrator")

# Deactivate user (keeps data, prevents login)
user_manager.deactivate_user(user_id=3)

# Reactivate user
user_manager.activate_user(user_id=3)
```

### Password Requirements

The system enforces the following password policy:

- **Minimum length**: 8 characters
- **Complexity**: Must include:
  - At least one uppercase letter (A-Z)
  - At least one lowercase letter (a-z)
  - At least one number (0-9)
  - At least one special character (!@#$%^&*)

**Example valid passwords:**
- `SecurePass123!`
- `MyP@ssw0rd`
- `Admin#2024`

### Troubleshooting Authentication

**"Invalid username or password"**
- Usernames are case-sensitive
- Check for typos in username
- Ensure caps lock is off for password
- Verify account is active: `SELECT * FROM users WHERE username='your_username';`

**"Account locked"**
- Too many failed login attempts
- Wait for lockout period to expire (check `account_locked_until` in database)
- Or ask admin to reset: `UPDATE users SET failed_login_attempts=0, account_locked_until=NULL WHERE username='your_username';`

**"No users exist"**
- Run `python initialize_auth.py` to create first admin
- Check database connection in `.env`

**"Must change password"**
- Contact administrator to reset `must_change_password` flag
- Or implement password change feature (future enhancement)

### Two-Factor Authentication (2FA)

**Overview**

The application uses Time-based One-Time Password (TOTP) authentication for enhanced security. 2FA is **mandatory for all administrators** and optional for other users.

**Supported Authenticator Apps:**
- Google Authenticator (Android/iOS)
- Microsoft Authenticator (Android/iOS)
- Authy (Android/iOS/Desktop)
- Any TOTP-compatible authenticator

**Setup Process (First Admin Login):**

1. **Login with username and password**
2. **Mandatory Setup Dialog appears** (administrators only)
3. **Scan QR Code**:
   - Open your authenticator app
   - Tap "+" or "Add Account"
   - Scan the QR code displayed on screen
   - Account will be added as "Store Manager - [your_username]"
4. **Save Backup Codes**:
   - 8 single-use backup codes are displayed
   - **Save these securely** (screenshot, print, or write down)
   - Each code can only be used once
5. **Verify Setup**:
   - Enter the 6-digit code from your authenticator app
   - Click "Enable 2FA"
   - Setup is complete!

**Using 2FA During Login:**

1. Enter username and password as normal
2. **After password verification**, 2FA dialog appears
3. Open your authenticator app
4. Enter the current 6-digit code (refreshes every 30 seconds)
5. Click "Verify"
6. Login complete!

**Using Backup Codes:**

If you don't have access to your authenticator app:

1. During 2FA verification, click "Use Backup Code"
2. Enter one of your 8 backup codes
3. Code will be consumed after use
4. Regenerate codes via Security Settings

**Managing 2FA Settings:**

After login, access Security Settings:
1. Click the **⚙ gear icon** (top right corner of dashboard)
2. Select **"Security Settings (2FA)"**
3. Available options:
   - **View QR Code**: Re-display QR code to add to another device
   - **View Backup Codes**: See remaining unused backup codes
   - **Regenerate Backup Codes**: Create new set of 8 codes (invalidates old ones)
   - **Disable 2FA**: Turn off 2FA (not recommended for administrators)

**2FA Enforcement Rules:**

| User Role | 2FA Requirement | Can Disable? |
|-----------|----------------|--------------|
| Administrator | **Mandatory** | Not recommended (security risk) |
| Manager | Optional | Yes |
| Employee | Optional | Yes |

**Security Best Practices:**

- ✅ **Save backup codes** immediately during setup
- ✅ **Store backup codes securely** (password manager, safe location)
- ✅ **Add to multiple devices** (backup phone, tablet)
- ✅ **Regenerate backup codes** if you suspect compromise
- ❌ **Don't share** QR codes or backup codes with anyone
- ❌ **Don't screenshot** QR codes and share via insecure channels
- ❌ **Don't disable 2FA** if you're an administrator

**Troubleshooting 2FA:**

**"Invalid verification code"**
- Codes expire every 30 seconds - use the current code
- Check your device's time is correct (TOTP requires accurate time)
- Ensure you're looking at the correct account in your authenticator app

**"Lost access to authenticator app"**
- Use one of your backup codes to login
- After login, go to Security Settings to regenerate codes or reset 2FA
- Contact another administrator if you're locked out

**"QR code won't scan"**
- Try manual entry: Click "Manual Entry" in your authenticator app
- Enter the secret key displayed below the QR code
- Account name: Store Manager
- Username: [your_username]

**"Need to reset 2FA for a user" (Admin only)**
```sql
-- Connect to MySQL
mysql -u root -p

USE store_manager;

-- Disable 2FA for a user (emergency only)
UPDATE users 
SET two_factor_enabled = FALSE, 
    two_factor_secret = NULL, 
    backup_codes = NULL 
WHERE username = 'locked_out_user';
```

## Project Structure

```
ETL/
├── data/
│   ├── CSV/                       # CSV data sources
│   ├── API/                       # API exports
│   ├── print/                     # Generated PDF reports
│   ├── data_model.md
│   └── etl_data_model_diagram.mmd
├── gui/
│   ├── login_window/              # Authentication UI
│   │   ├── window.py
│   │   ├── worker.py
│   │   └── ui_components.py
│   ├── dashboard_window/          # Main dashboard
│   │   ├── window.py             # Orchestration
│   │   ├── ui_builder.py         # UI construction
│   │   ├── data_handler.py       # Data operations
│   │   ├── worker.py
│   │   ├── ui_components.py
│   │   └── gauge_widget.py       # Sales visualization
│   ├── admin_window/              # Database management
│   │   ├── window.py             # Orchestration
│   │   ├── ui_builder.py         # UI construction
│   │   ├── operation_handler.py  # ETL operations
│   │   ├── worker.py
│   │   └── ui_components.py
│   ├── user_management/           # User CRUD
│   │   ├── user_management_dialog.py
│   │   ├── create_user_widget.py
│   │   └── manage_users_widget.py
│   ├── two_factor_setup_dialog.py # 2FA setup/management
│   ├── two_factor_verify_dialog.py # 2FA login verification
│   ├── tabbed_window.py           # Window container
│   ├── base_worker.py             # Base worker thread
│   ├── path_config.py
│   └── themes/                    # Theme system
│       ├── base_theme.py
│       ├── dark_theme.py
│       ├── light_theme.py
│       ├── theme_manager.py
│       └── img/
│           └── logo.png           # App icon
├── src/
│   ├── auth/                      # Authentication system
│   │   ├── user_manager.py        # Main facade
│   │   ├── password_handler.py    # bcrypt hashing
│   │   ├── password_policy.py     # Complexity rules
│   │   ├── user_authenticator.py  # Login logic
│   │   ├── user_repository.py     # Database CRUD
│   │   ├── two_factor_auth.py     # 2FA TOTP logic
│   │   ├── session.py             # Session management
│   │   ├── session_timeout.py     # Auto timeout
│   │   ├── account_lockout.py     # Brute-force protection
│   │   ├── permissions.py         # Role-based access
│   │   └── migration_*.py         # Schema migrations
│   ├── api/                       # API client
│   │   ├── api_client.py
│   │   ├── api_models.py
│   │   ├── rate_limiter.py
│   │   ├── retry_handler.py
│   │   └── data_processor.py
│   ├── config/                    # Configuration
│   │   ├── database.py
│   │   ├── api.py
│   │   └── environments.py
│   ├── database/                  # Database operations
│   │   ├── db_manager.py          # Main database facade
│   │   ├── connection_manager.py  # Connection pooling
│   │   ├── csv_operations.py      # CSV import/export
│   │   ├── data_from_api.py       # API data handling
│   │   ├── pdf_generator.py       # PDF reports
│   │   ├── schema_manager.py      # Table creation
│   │   ├── data_validator.py      # Data integrity
│   │   ├── pandas_optimizer.py    # Performance tuning
│   │   ├── batch_operations/
│   │   └── utilities/
│   ├── exceptions/                # Exception handling
│   │   └── etl_exceptions.py
│   └── logs/
│       └── logging_system.py
├── logs/
│   └── etl_structured.json        # Application logs
├── tests/
│   ├── run_tests.py
│   ├── test_api_csv_export.py
│   └── test_csv_access.py
├── run_app.py                     # Main entry (with auth)
├── run_admin_direct.py            # Direct admin (no auth)
├── initialize_auth.py             # Auth setup
├── clean_logs.ps1                 # Log cleanup utility
├── cache_cleaner.py               # Cache management
├── REFACTORING_SUMMARY.md         # Code refactoring details
├── 2FA_IMPLEMENTATION.md          # 2FA documentation
├── 2FA_INTEGRATION_OPTIONS.md     # 2FA setup guide
├── SECURITY_ENHANCEMENT_OPTIONS.md # Security features
└── README.md                      # This file
```

### Architecture Highlights

**Refactored Components** (November 2024):
- **Dashboard Window**: Reduced from 592 → 211 lines (64% reduction)
  - `window.py` - Main orchestration (211 lines)
  - `ui_builder.py` - UI construction (206 lines)
  - `data_handler.py` - Data operations & event handling (270 lines)
  - `worker.py` - Async database operations (313 lines)
  - `gauge_widget.py` - Sales visualization widget (97 lines)
  
- **Admin Window**: Reduced from 404 → 147 lines (64% reduction)
  - `window.py` - Main orchestration (147 lines)
  - `ui_builder.py` - UI construction (200 lines)
  - `operation_handler.py` - ETL operations logic (287 lines)
  - `worker.py` - Async ETL operations (385 lines)

**Connection Management** (November 2024):
- **Dual-Driver Support**: Compatible with both PyMySQL and mysql.connector
  - Automatic fallback pattern for cursor creation
  - Try mysql.connector `cursor(dictionary=True)` first
  - Fall back to PyMySQL `cursor(pymysql.cursors.DictCursor)` if needed
- **Connection Pooling**: Native MySQL connection pool with proper release
  - Pool size: 5 connections (configurable)
  - Automatic connection return to pool via `conn.close()`
  - Context manager pattern for safe resource management
- **Race Condition Fix**: 150ms delayed sales fetch prevents UI initialization conflicts
  - Ensures widgets are ready before data loads
  - Works consistently across all user roles

**2FA Flow**:
- Login → Check 2FA status → Verify TOTP code → Create session → Dashboard
- Setup QR → Scan with authenticator → Verify code → Generate backup codes → Save

**Worker Thread Pattern**:
- Async operations use QThread with signals/slots
- Progress updates via Qt signals
- Non-blocking UI during database operations
- Proper worker cleanup with deleteLater()

See `REFACTORING_SUMMARY.md` for detailed refactoring documentation.

## Database Configuration

### Quick Setup

**Create `.env` file in project root:**
```
DB_USER=root
DB_PASSWORD=your_mysql_password_here
DB_HOST=127.0.0.1
DB_NAME=store_manager
```

### Database Schema

The application manages **10 tables** with proper foreign key relationships:

**Core Business Tables:**
- `categories` - Product categories
- `brands` - Product brands
- `products` - Product catalog (links to categories, brands)
- `stores` - Store locations
- `staffs` - Employee and manager data
- `stocks` - Inventory levels (links to products, stores)

**Transaction Tables:**
- `customers` - Customer information
- `orders` - Order headers (links to customers, stores, staffs)
- `order_items` - Order line items (links to orders, products)

**Security Table:**
- `users` - Authentication and authorization (links to staffs)

### Automatic Setup

When you run `initialize_auth.py` or launch the app for the first time:

1. **Database Creation**: If `store_manager` doesn't exist, it will be created
2. **Table Creation**: All 10 tables will be created with proper schema
3. **Data Validation**: Schema is validated on each connection

### Manual Database Operations

**Connect to MySQL directly:**
```bash
mysql -u root -p
```

**View tables:**
```sql
USE store_manager;
SHOW TABLES;
DESCRIBE users;
```

**Check data:**
```sql
SELECT * FROM users;
SELECT * FROM products LIMIT 10;
```

### Connection Details

- **Driver**: Dual-driver support (PyMySQL primary, mysql.connector fallback)
- **Connection Pool**: Native MySQL pooling with 5 connections
  - Automatic connection recycling
  - Thread-safe connection management
  - Context manager pattern for safety
- **Port**: 3306 (default MySQL port)
- **Charset**: utf8mb4 with utf8mb4_unicode_ci collation
- **Engine**: InnoDB (ACID-compliant transactions)
- **Cursor Type**: Dictionary cursors for easy data access
  - PyMySQL: `DictCursor` (returns dict rows)
  - mysql.connector: `dictionary=True` (returns dict rows)
  - Automatic fallback between drivers
- **Engine**: InnoDB (supports foreign keys and transactions)
- **Connection Pooling**: Managed automatically
- **Auto-commit**: Disabled (explicit transaction control)
- **Character Set**: utf8mb4 (full Unicode support)

## Architecture

### Application Flow
```
run_app.py → LoginWindow → TwoFactorVerifyDialog → TabbedMainWindow
                                                         ├── DashboardWindow (all roles)
                                                         ├── ETLMainWindow (Admin only)
                                                         └── UserManagementDialog (Admin only)
```

### GUI Components (Refactored Architecture)

**Dashboard Window** (Clean separation of concerns):
```
DashboardMainWindow (211 lines)
    ├── DashboardUIBuilder (205 lines)
    │   ├── Database management section
    │   ├── Employee list (Manager/Admin)
    │   ├── Content section (tables + gauge)
    │   ├── Logout section
    │   └── Toolbar with settings
    │
    └── DashboardDataHandler (271 lines)
        ├── Customer loading
        ├── Employee loading
        ├── PDF generation
        ├── Sales data fetching
        └── Worker thread management
```

**Admin Window** (Clean separation of concerns):
```
ETLMainWindow (147 lines)
    ├── AdminUIBuilder (200 lines)
    │   ├── API configuration section
    │   ├── File selection section
    │   ├── Data loading section
    │   ├── Database operations section
    │   └── Test operations section
    │
    └── AdminOperationHandler (287 lines)
        ├── Status initialization
        ├── Operation execution
        ├── CSV file selection
        ├── API data loading
        └── Worker thread management
```

### Authentication System
```
UserManager (Facade)
    ├── PasswordHandler → bcrypt hashing
    ├── UserAuthenticator → login validation
    ├── TwoFactorAuth → TOTP generation/verification
    ├── UserRepository → database CRUD
    ├── SessionManager → active session tracking
    ├── AccountLockout → brute-force protection
    └── PasswordPolicy → complexity enforcement
```

### Two-Factor Authentication Flow
```
1. User enters credentials → UserAuthenticator validates
2. If Admin → Check 2FA enabled
   - Not enabled → Force setup with TwoFactorSetupDialog
   - Enabled → Show TwoFactorVerifyDialog
3. Verify TOTP code or backup code → TwoFactorAuth.verify_code()
4. Success → SessionManager.start_session()
```

### Theme System
```
ThemeManager (Singleton)
    ├── BaseTheme (Abstract)
    ├── LightTheme → Professional light colors
    └── DarkTheme → Professional dark colors
    
Applied via: app.setStyleSheet(theme.get_stylesheet())
```

### Database Operations
```
DatabaseManager
    ├── ConnectionManager → PyMySQL connection pooling
    ├── CSVOperations → pandas import/export with validation
    ├── SchemaManager → table creation/migration
    ├── PDFGenerator → reportlab customer reports
    ├── DataValidator → schema alignment checks
    └── BatchOperations → optimized bulk insert/update
```

### Worker Thread Pattern
```
BaseWorker (QThread)
    ├── DashboardWorker → fetch_customers, fetch_employees, fetch_sales
    └── ETLWorker → load_csv, load_api, create_tables, test_connection
    
Signals: progress, finished, error, data_ready
Slots: cancel() for graceful shutdown
```

## Technical Notes

### Recent Bug Fixes (November 2024)

**Connection Pool Exhaustion Issue**:
- **Problem**: MySQL native connection pool connections were not being returned after use
- **Cause**: `_release()` method in `connection_manager.py` returned early for native pools
- **Solution**: Modified `_release()` to call `conn.close()` for native pools, properly returning connections to the pool
- **Impact**: Fixed "pool exhausted" errors when multiple workers run simultaneously

**Cursor API Compatibility**:
- **Problem**: Application uses both PyMySQL and mysql.connector drivers with different cursor APIs
- **Cause**: `connect.py` prioritizes PyMySQL, but code used mysql.connector syntax `cursor(dictionary=True)`
- **Solution**: Implemented dual-driver fallback pattern in all cursor creation:
  ```python
  try:
      cursor = conn.cursor(dictionary=True)  # mysql.connector
  except TypeError:
      import pymysql.cursors
      cursor = conn.cursor(pymysql.cursors.DictCursor)  # PyMySQL
  ```
- **Files Updated**: `worker.py`, `user_authenticator.py`, `password_handler.py`, `user_repository.py`
- **Impact**: Works seamlessly with either database driver installed

**Sales Gauge Race Condition**:
- **Problem**: Sales gauge would randomly not display on different user roles
- **Cause**: Multiple workers starting simultaneously created timing issues where data arrived before widget was ready
- **Solution**: Delayed sales fetch by 150ms in `initialize_dashboard()` to ensure UI is fully initialized
- **Impact**: Consistent gauge display across all roles (Employee, Manager, Administrator)

### Performance Considerations

**Connection Pooling**:
- Pool size: 5 connections (sufficient for typical concurrent operations)
- Maximum simultaneous workers: 4 (tables, customers, employees, sales)
- Connection reuse reduces overhead and prevents exhaustion

**Worker Thread Management**:
- All database operations run in background threads (QThread)
- UI remains responsive during long operations
- Workers automatically cleaned up with `deleteLater()`

**Data Loading Strategy**:
- Tables list: Immediate fetch on dashboard load
- Customer dropdown: Immediate fetch (needed for all roles)
- Employee list: Conditional fetch (Manager/Admin only)
- Sales gauge: Delayed 150ms fetch (prevents race condition)

## Testing

```bash
cd tests && python run_tests.py
python src/cache_cleaner.py
````

## Contact

**Project Maintainer:** Andy Sylvia Rosenvold  
**Email:** andy.rosenvold@specialisterne.com  
**Repository:** https://github.com/Sylverose/Store-Database-Management-System

## License

MIT License - see LICENSE file for details



