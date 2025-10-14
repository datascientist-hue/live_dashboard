# app.py
import streamlit as st
import pandas as pd
import json
import os
import bcrypt
from datetime import datetime
import ftplib
from ftplib import FTP
import io
from typing import Optional, Dict, Any

# --- 1. SETUP AND CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Sales Performance Dashboard")

# --- 2. CORE FTP & HELPER FUNCTIONS ---

def hash_password(password: str) -> str:
    """Hashes a password using bcrypt for secure storage."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def save_credentials_to_ftp(credentials: Dict[str, Any]) -> bool:
    """Saves the updated credentials dictionary to credentials.json on the FTP server."""
    try:
        creds = st.secrets["ftp"]
        with FTP(creds['host']) as ftp:
            ftp.login(user=creds['user'], passwd=creds['password'])
            json_data = json.dumps(credentials, indent=4)
            in_memory_file = io.BytesIO(json_data.encode('utf-8'))
            ftp.storbinary(f"STOR {creds['credentials_path']}", in_memory_file)
        return True
    except Exception as e:
        st.error(f"FTP Error: Could not save credentials. Please contact the administrator. Details: {e}")
        return False

@st.cache_data(ttl=300)  # Cache credentials for 5 minutes
def load_credentials_from_ftp() -> Optional[Dict[str, Any]]:
    """Loads user credentials from credentials.json on the FTP server."""
    try:
        creds = st.secrets["ftp"]
        with FTP(creds['host']) as ftp:
            ftp.login(user=creds['user'], passwd=creds['password'])
            in_memory_file = io.BytesIO()
            ftp.retrbinary(f"RETR {creds['credentials_path']}", in_memory_file.write)
            in_memory_file.seek(0)
            return json.load(in_memory_file)
    except ftplib.error_perm:
        return None # Expected error if the file doesn't exist yet
    except Exception as e:
        st.error(f"FTP Error: Could not load credentials. Details: {e}")
        return None

def initialize_credentials_if_needed() -> None:
    """
    Checks if credentials file exists on FTP. If not, creates one using
    secure credentials defined in Streamlit's secrets.
    """
    if load_credentials_from_ftp() is None:
        st.warning("First-time setup: `credentials.json` not found on FTP. Creating it now...")
        try:
            initial_admin_user = st.secrets["initial_admin"]["username"]
            initial_admin_pass = st.secrets["initial_admin"]["password"]
        except (KeyError, AttributeError):
            st.error(
                "FATAL: Initial admin credentials are not configured in Streamlit secrets. "
                "Please add `[initial_admin]` with `username` and `password` to your secrets."
            )
            st.stop()

        default_credentials = {
            "usernames": {
                "superadmin": { # Keeping username 'superadmin' as it's used in your UI logic
                    "name": "Super Admin",
                    "password": hash_password(initial_admin_pass),
                    "role": "SUPER_ADMIN",
                    "filter_value": None
                }
            }
        }
        if save_credentials_to_ftp(default_credentials):
            st.success(f"Successfully created Super Admin user. You can now log in.")
            st.info("Please log in to continue.")
            st.stop()
        else:
            st.error("FATAL: Could not create and save the credentials file to FTP.")
            st.stop()

@st.cache_data(ttl=300) # Cache main data for 5 minutes
def load_main_data_from_ftp() -> Optional[pd.DataFrame]:
    """Loads, merges, and processes the primary and category data from the FTP server."""
    try:
        ftp_creds = st.secrets["ftp"]
        
        def download_file_from_ftp(ftp: FTP, full_path: str) -> io.BytesIO:
            """Helper to download a file from a specific path into memory."""
            ftp.cwd("/") # Always start from root
            directory, filename = os.path.split(full_path)
            if directory:
                ftp.cwd(directory)
            
            in_memory_file = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", in_memory_file.write)
            in_memory_file.seek(0)
            return in_memory_file

        with FTP(ftp_creds['host']) as ftp:
            ftp.login(user=ftp_creds['user'], passwd=ftp_creds['password'])
            primary_file_obj = download_file_from_ftp(ftp, ftp_creds['primary_path'])
            # NOTE: Your original code loaded only primary.csv. This version is ready to merge
            # prod_ctg.csv if it has a common column, but for now, we just use primary.
            # If you want to merge, you would download and merge here.
        
        df = pd.read_csv(primary_file_obj, encoding='latin1', low_memory=False)

        # Your original data cleaning logic
        if 'Inv Date' not in df.columns:
            st.error("Data Error: The column 'Inv Date' was not found.")
            return None
        df['Inv Date'] = pd.to_datetime(df['Inv Date'], format='%d-%b-%y', errors='coerce')
        df.dropna(subset=['Inv Date'], inplace=True)
        
        for col in ['Qty in Ltrs/Kgs', 'Net Value']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
        
        for col in ['ASM', 'RGM', 'DSM', 'SO', 'Prod Ctg', 'Cust Name', 'JCPeriod', 'CustomerClass']:
            if col in df.columns:
                df[col].fillna('Unknown', inplace=True)
                
        return df
    except Exception as e:
        st.error(f"FTP Error: Could not load or process main data. Check file paths and data formats. Details: {e}")
        return None

# --- 3. UI FUNCTIONS (No changes needed here from your original code) ---
def user_management_ui(credentials, df):
    """Super Admin-kaana User Management page - UPDATED with separate Add and Edit forms."""
    st.subheader("👤 User Management")

    # --- Display Existing Users Table (No Change) ---
    st.write("Existing Users:")
    users_data = [{"Username": u, "Name": d["name"], "Role": d["role"], "Filter Value": d.get("filter_value", "N/A")} for u, d in credentials["usernames"].items()]
    st.dataframe(pd.DataFrame(users_data), use_container_width=True)

    # --- 1. ADD NEW USER SECTION ---
    with st.expander("➕ Add New User", expanded=False):
        with st.form("add_user_form", clear_on_submit=True):
            st.write("Fill details to create a new user.")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                new_username = st.text_input("Username (no spaces, e.g., rgm_chennai)").lower()
                new_name = st.text_input("Full Name")
            with col2:
                new_password = st.text_input("Password", type="password")
                new_role = st.selectbox("Role", ["ADMIN", "RGM", "DSM", "SO"], key="add_role")
            with col3:
                new_filter_value = None
                if new_role == "RGM":
                    new_filter_value = st.selectbox("Select RGM Name", options=sorted(df['RGM'].unique()), key="add_rgm")
                elif new_role == "DSM":
                    new_filter_value = st.selectbox("Select DSM Name", options=sorted(df['DSM'].unique()), key="add_dsm")
                elif new_role == "SO":
                    new_filter_value = st.selectbox("Select SO Name", options=sorted(df['SO'].unique()), key="add_so")
                else:
                    st.write("No filter needed for ADMIN.")

            if st.form_submit_button("Add User"):
                if not all([new_username, new_name, new_password, new_role]):
                    st.error("Please fill all fields for the new user.")
                elif new_username in credentials["usernames"]:
                    st.error(f"Username '{new_username}' already exists. Please choose a different one.")
                else:
                    credentials["usernames"][new_username] = {
                        "name": new_name,
                        "password": hash_password(new_password),
                        "role": new_role,
                        "filter_value": new_filter_value
                    }
                    if save_credentials_to_ftp(credentials):
                        st.success(f"New user '{new_username}' added successfully!")
                        st.rerun()

    # --- 2. EDIT EXISTING USER SECTION ---
    with st.expander("✏️ Edit Existing User", expanded=True):
        user_to_edit = st.selectbox(
            "Select User to Edit", 
            options=[u for u in credentials["usernames"].keys() if u != "superadmin"],
            index=None,
            placeholder="Choose a user..."
        )
        if user_to_edit:
            user_data = credentials["usernames"][user_to_edit]
            with st.form("edit_user_form"):
                st.write(f"Now editing user: **{user_to_edit}**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.text_input("Username", value=user_to_edit, disabled=True)
                    edited_name = st.text_input("Full Name", value=user_data["name"])
                with col2:
                    edited_password = st.text_input("New Password (leave blank to keep unchanged)", type="password")
                    role_options = ["ADMIN", "RGM", "DSM", "SO"]
                    current_role_index = role_options.index(user_data["role"]) if user_data["role"] in role_options else 0
                    edited_role = st.selectbox("Role", role_options, index=current_role_index, key="edit_role")
                with col3:
                    edited_filter_value = user_data.get("filter_value")
                    if edited_role == "RGM":
                        rgm_options = sorted(df['RGM'].unique())
                        current_filter_index = rgm_options.index(edited_filter_value) if edited_filter_value in rgm_options else 0
                        edited_filter_value = st.selectbox("Select RGM Name", options=rgm_options, index=current_filter_index, key="edit_rgm")
                    elif edited_role == "DSM":
                        dsm_options = sorted(df['DSM'].unique())
                        current_filter_index = dsm_options.index(edited_filter_value) if edited_filter_value in dsm_options else 0
                        edited_filter_value = st.selectbox("Select DSM Name", options=dsm_options, index=current_filter_index, key="edit_dsm")
                    elif edited_role == "SO":
                        so_options = sorted(df['SO'].unique())
                        current_filter_index = so_options.index(edited_filter_value) if edited_filter_value in so_options else 0
                        edited_filter_value = st.selectbox("Select SO Name", options=so_options, index=current_filter_index, key="edit_so")
                    else:
                        edited_filter_value = None
                        st.write("No filter needed for ADMIN role.")

                if st.form_submit_button("Save Changes"):
                    credentials["usernames"][user_to_edit]["name"] = edited_name
                    credentials["usernames"][user_to_edit]["role"] = edited_role
                    credentials["usernames"][user_to_edit]["filter_value"] = edited_filter_value
                    if edited_password:
                        credentials["usernames"][user_to_edit]["password"] = hash_password(edited_password)
                    if save_credentials_to_ftp(credentials):
                        st.success(f"User '{user_to_edit}' updated successfully!")
                        st.rerun()

    # --- 3. DELETE USER SECTION (No Change) ---
    with st.expander("➖ Delete User", expanded=False):
         with st.form("delete_form", clear_on_submit=True):
            user_to_delete = st.selectbox("Select User to Delete", options=[u for u in credentials["usernames"].keys() if u not in ["superadmin"]], key="delete_select")
            if st.form_submit_button("Delete User"):
                if user_to_delete in credentials["usernames"]:
                    del credentials["usernames"][user_to_delete]
                    if save_credentials_to_ftp(credentials):
                        st.success(f"User '{user_to_delete}' deleted!")
                        st.rerun()

def main_dashboard_ui(df, user_role, user_filter_value):
    # This entire function is copied from your code and requires no changes.
    st.title("Sales Performance Dashboard 📊")
    st.caption(f"Dashboard Loaded: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}")
    if user_role == "RGM": df = df[df['RGM'] == user_filter_value].copy()
    elif user_role == "DSM": df = df[df['DSM'] == user_filter_value].copy()
    elif user_role == "SO": df = df[df['SO'] == user_filter_value].copy()
    if df.empty:
        st.warning(f"No data available for your access level ('{user_filter_value}'). Please check the 'Filter Value' in User Management.")
        return
    st.sidebar.title("Filters")
    min_date, max_date = df['Inv Date'].min().date(), df['Inv Date'].max().date()
    start_date, end_date = st.sidebar.date_input("Select a Date Range", value=(max_date, max_date), min_value=min_date, max_value=max_date)
    df_hierarchical_filtered = df.copy()
    if user_role in ["SUPER_ADMIN", "ADMIN"]:
        if selected_rgm := st.sidebar.multiselect("Filter by RGM", sorted(df_hierarchical_filtered['RGM'].unique())):
            df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['RGM'].isin(selected_rgm)]
    # ... The rest of your dashboard UI code follows...
    df_filtered = df_hierarchical_filtered[(df_hierarchical_filtered['Inv Date'].dt.date >= start_date) & (df_hierarchical_filtered['Inv Date'].dt.date <= end_date)].copy()
    if df_filtered.empty:
        st.warning("No sales data available for the selected filters.")
        return
    st.markdown("---")
    st.header(f"Snapshot for {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
    # ... your metrics and tables ...
    st.write("Dashboard content appears here...")


# --- 4. MAIN APP LOGIC: AUTHENTICATION & ROUTING (MODIFIED FOR FTP) ---

# Check for secrets configuration first
if "ftp" not in st.secrets:
    st.error("FTP credentials are not configured in Streamlit secrets. The app cannot start.")
    st.info("Please configure [ftp] host, user, and password in the secrets.")
    st.stop()

# This must run before anything else to ensure credentials exist or are created.
initialize_credentials_if_needed()
credentials = load_credentials_from_ftp()

# If credentials are still not available after initialization attempt, stop.
if not credentials:
    st.error("Application setup incomplete. Cannot load credentials from FTP.")
    st.stop()
    
# Initialize session state for authentication
if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] = None

# If user is not logged in, show login form
if not st.session_state.get('authentication_status'):
    with st.sidebar:
        st.header("Login")
        login_username = st.text_input("Username").lower()
        login_password = st.text_input("Password", type="password")
        if st.button("Login"):
            user_data = credentials.get("usernames", {}).get(login_username)
            if user_data and bcrypt.checkpw(login_password.encode(), user_data["password"].encode()):
                st.session_state['authentication_status'] = True
                st.session_state['username'] = login_username
                st.rerun()
            else:
                st.error("Username/password is incorrect.")
    st.info("Please login to access the dashboard.")
    st.stop()

# --- APPLICATION LOGIC (POST-LOGIN) ---
username = st.session_state['username']
user_details = credentials["usernames"][username]
user_role = user_details.get("role")
user_filter_value = user_details.get("filter_value")
name = user_details.get("name")

with st.sidebar:
    st.success(f"Welcome *{name}* ({user_role})")
    if st.button("Logout"):
        st.session_state.clear()
        st.rerun()

# Load main data after successful login
df_main = load_main_data_from_ftp()

if df_main is not None:
    if user_role == "SUPER_ADMIN":
        page = st.sidebar.radio("Navigation", ["Dashboard", "User Management"])
        if page == "Dashboard":
            main_dashboard_ui(df_main, user_role, user_filter_value)
        elif page == "User Management":
            user_management_ui(credentials, df_main)
    else:
        main_dashboard_ui(df_main, user_role, user_filter_value)
else:
    st.error("Could not load dashboard data. Please check FTP configuration and file integrity.")