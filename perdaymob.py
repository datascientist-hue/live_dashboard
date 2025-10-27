# app.py
import streamlit as st
import pandas as pd
import json
import os
import bcrypt  # Required for password hashing
from datetime import datetime, timedelta
import ftplib
from ftplib import FTP  # Required to connect to the FTP server
import io  # Required to handle files in memory
from zoneinfo import ZoneInfo # Required for timezone conversion
import time
import streamlit_authenticator as stauth

# --- 1. APP CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Sales Dashboard")


# --- 2. FTP-BASED HELPER FUNCTIONS FOR USER MANAGEMENT (NO CHANGE) ---

def load_credentials_from_ftp():
    """Loads user data from the credentials.json file on the FTP server."""
    try:
        creds = st.secrets["ftp"]
        ftp = FTP(creds['host'])
        ftp.login(user=creds['user'], passwd=creds['password'])
        
        in_memory_file = io.BytesIO()
        ftp.retrbinary(f"RETR {creds['credentials_path']}", in_memory_file.write)
        in_memory_file.seek(0)
        ftp.quit()
        
        return json.load(in_memory_file)
        
    except ftplib.error_perm:
        return None
    except Exception as e:
        st.error(f"FTP Error: Could not load login credentials: {e}")
        return None

def save_credentials_to_ftp(credentials):
    """Saves new user data to the credentials.json file on the FTP server."""
    try:
        creds = st.secrets["ftp"]
        ftp = FTP(creds['host'])
        ftp.login(user=creds['user'], passwd=creds['password'])
        
        json_data = json.dumps(credentials, indent=4)
        in_memory_file = io.BytesIO(json_data.encode('utf-8'))
        
        ftp.storbinary(f"STOR {creds['credentials_path']}", in_memory_file)
        ftp.quit()
        return True
    except Exception as e:
        st.error(f"FTP Error: Could not save credentials: {e}")
        return False

def hash_password(password):
    """Hashes the password using bcrypt to make it secure."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def initialize_credentials_if_needed():
    """
    If credentials.json does not exist on the FTP, it creates one
    with a default superadmin user from Streamlit secrets.
    """
    if load_credentials_from_ftp() is None:
        st.warning("`credentials.json` not found on FTP. A new file is being created with a Super Admin.")
        
        try:
            initial_admin_pass = st.secrets["initial_admin"]["password"]
        except (KeyError, AttributeError):
            st.error("FATAL ERROR: `initial_admin` password is not configured in Streamlit secrets. The app cannot start.")
            st.stop()

        default_credentials = {
            "credentials": {
                "usernames": {
                    "superadmin": {
                        "email": "superadmin@example.com",
                        "name": "Super Admin",
                        "password": hash_password(initial_admin_pass),
                        "role": "SUPER_ADMIN",
                        "filter_value": None
                    }
                }
            },
            "cookie": {
                "expiry_days": 30,
                "key": "a_unique_and_random_secret_key", 
                "name": "sales_dashboard_cookie"
            }
        }
        
        if save_credentials_to_ftp(default_credentials):
            st.success("Default Super Admin has been created. Please log in.")
            st.rerun()
        else:
            st.error("FATAL ERROR: Could not create the credentials file on the FTP server.")
            st.stop()

# --- 3. FTP-BASED DATA LOADING FUNCTION (MODIFIED TO HANDLE NEW FILE STRUCTURE) ---

def download_and_read_parquet_with_retry(ftp_connection, path, max_retries=3, delay=5):
    """
    Tries to download and read a parquet file with retries for race conditions.
    """
    for attempt in range(max_retries):
        try:
            in_memory_file = io.BytesIO()
            ftp_connection.retrbinary(f"RETR {path}", in_memory_file.write)
            
            if in_memory_file.getbuffer().nbytes == 0:
                st.warning(f"Warning: File at path '{path}' is empty (0 KB).")
                return None

            in_memory_file.seek(0)
            df = pd.read_parquet(in_memory_file)
            return df
        
        except Exception as e:
            st.toast(f"Attempt {attempt + 1}/{max_retries} to read '{os.path.basename(path)}' failed. Retrying in {delay}s...", icon="⏳")
            if attempt + 1 < max_retries:
                time.sleep(delay)
            else:
                raise e
    return None


@st.cache_data(ttl=300)
def load_main_data_from_ftp():
    """
    Loads primary data from FTP. The mapping logic has been REMOVED as the new 
    primary file contains all necessary columns like 'upd_prod_ctg'.
    """
    modification_time_str = None
    status_msg = None
    try:
        ftp_creds = st.secrets["ftp"]
        
        with FTP(ftp_creds['host']) as ftp:
            ftp.login(user=ftp_creds['user'], passwd=ftp_creds['password'])
            
            try:
                mdtm_response = ftp.sendcmd(f"MDTM {ftp_creds['primary_path']}")
                modification_time_str = mdtm_response.split(' ')[1]
            except ftplib.all_errors:
                pass

            # --- CHANGE: We now ONLY download and use the primary data file. ---
            # The category mapping file is no longer needed for this logic.
            df = download_and_read_parquet_with_retry(ftp, ftp_creds['primary_path'])

            if df is None:
                return None, None, "Data Error: Could not load the main data file from FTP after multiple attempts. The file might be locked or empty.", None
        
        # --- CHANGE: REMOVED the old mapping logic block. ---
        # The code now assumes 'upd_prod_ctg' and other columns exist directly in the df.
        
        if 'InvDate' not in df.columns:
            return None, None, "Data Error: The column 'InvDate' was not found.", None
        
        df['InvDate'] = pd.to_datetime(df['InvDate'], format='%Y-%m-%d', errors='coerce')
        df.dropna(subset=['InvDate'], inplace=True)

        today = pd.to_datetime(datetime.now().date())
        start_date_filter = today - timedelta(days=45)
        df = df[df['InvDate'] >= start_date_filter].copy()
        status_msg = "Showing data from the last 45 days for faster performance."

        numeric_cols = ['PrimaryQtyInLtrs/Kgs', 'PrimaryLineTotalBeforeTax', 'PrimaryQtyinNos', 'PrimaryQtyinCases/Bags']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
        
        # --- CHANGE: Added new columns from your list to the cleaning process ---
        key_cols = ['ASM', 'RGM', 'DSM', 'SO', 'ProductCategory', 'BP Name', 'CustomerClass', 
                    'DocumentType', 'WhsCode', 'CustType', 'Brand', 'ProductGroup', 'upd_prod_ctg'] # Added new columns
        for col in key_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
        
        return df, modification_time_str, None, status_msg

    except ftplib.all_errors as e:
        error_msg = f"FTP Error: Could not connect or find the data file. Please check the path and credentials. Details: {e}"
        return None, None, error_msg, None
    except Exception as e:
        error_msg = f"Error after retries: Failed to load data from FTP. Details: {e}"
        return None, None, error_msg, None

# --- 4. UI FUNCTIONS (Minor change in 'Product Wise' view) ---

def user_management_ui(credentials, df):
    """UI for the Super Admin to manage users - with Add and Edit forms."""
    st.subheader("👤 User Management")
    user_dict = credentials['credentials']['usernames']

    st.write("Existing Users:")
    users_data = [{"Username": u, "Name": d["name"], "Role": d.get("role", "N/A"), "Filter Value": d.get("filter_value", "N/A")} for u, d in user_dict.items()]
    st.dataframe(pd.DataFrame(users_data), use_container_width=True, hide_index=True)

    with st.expander("➕ Add New User", expanded=False):
        with st.form("add_user_form", clear_on_submit=True):
            st.write("Fill details to create a new user.")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                new_username = st.text_input("Username (no spaces, e.g., rgm_chennai)").lower()
                new_name = st.text_input("Full Name")
            with col2:
                new_password = st.text_input("Password", type="password")
                new_role = st.selectbox("Role", ["ADMIN", "RGM", "DSM", "ASM", "SO"], key="add_role")
            with col3:
                new_filter_value = None
                if new_role == "RGM":
                    new_filter_value = st.selectbox("Select RGM Name", options=sorted(df['RGM'].unique()), key="add_rgm")
                elif new_role == "DSM":
                    new_filter_value = st.selectbox("Select DSM Name", options=sorted(df['DSM'].unique()), key="add_dsm")
                elif new_role == "ASM":
                    new_filter_value = st.selectbox("Select ASM Name", options=sorted(df['ASM'].unique()), key="add_Asm")
                elif new_role == "SO":
                    new_filter_value = st.selectbox("Select SO Name", options=sorted(df['SO'].unique()), key="add_so")
                else:
                    st.write("No filter needed for ADMIN.")

            if st.form_submit_button("Add User"):
                if not all([new_username, new_name, new_password, new_role]):
                    st.error("Please fill all fields for the new user.")
                elif new_username in user_dict:
                    st.error(f"Username '{new_username}' already exists. Please choose a different one.")
                else:
                    user_dict[new_username] = {
                        "email": f"{new_username}@example.com",
                        "name": new_name,
                        "password": hash_password(new_password),
                        "role": new_role,
                        "filter_value": new_filter_value
                    }
                    if save_credentials_to_ftp(credentials):
                        st.success(f"New user '{new_username}' added successfully!")
                        st.rerun()

    with st.expander("✏️ Edit Existing User", expanded=True):
        user_to_edit = st.selectbox(
            "Select User to Edit", 
            options=[u for u in user_dict.keys() if u != "superadmin"],
            index=None,
            placeholder="Choose a user..."
        )
        if user_to_edit:
            user_data = user_dict[user_to_edit]
            with st.form("edit_user_form"):
                st.write(f"Now editing user: **{user_to_edit}**")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.text_input("Username", value=user_to_edit, disabled=True)
                    edited_name = st.text_input("Full Name", value=user_data["name"])
                with col2:
                    edited_password = st.text_input("New Password (leave blank to keep unchanged)", type="password")
                    role_options = ["ADMIN", "RGM", "DSM", "ASM" ,"SO"]
                    current_role_index = role_options.index(user_data.get("role")) if user_data.get("role") in role_options else 0
                    edited_role = st.selectbox("Role", role_options, index=current_role_index, key="edit_role")
                with col3:
                    edited_filter_value = user_data.get("filter_value")
                    if edited_role == "RGM":
                        rgm_options = sorted(df['RGM'].unique())
                        current_filter_index = rgm_options.index(edited_filter_value) if edited_filter_value in rgm_options else 0
                        edited_filter_value = st.selectbox("Select RGM Name", options=rgm_options, index=current_filter_index, key="edit_rgm")
                    elif edited_role == "DSM":
                        dsm_options = sorted(df['DSM'].unique())
                        current_selection = user_data.get("filter_value")
                        default_selection = []
                        if isinstance(current_selection, list):
                            default_selection = [dsm for dsm in current_selection if dsm in dsm_options]
                        elif current_selection in dsm_options:
                             default_selection = [current_selection]
                        edited_filter_value = st.multiselect("Select DSM Name(s)", options=dsm_options, default=default_selection, key="edit_dsm")
                    elif edited_role == "ASM":
                        asm_options = sorted(df['ASM'].unique())
                        current_selection = user_data.get("filter_value")
                        default_selection = []
                        if isinstance(current_selection, list):
                            default_selection = [asm for asm in current_selection if asm in asm_options]
                        elif current_selection in asm_options:
                             default_selection = [current_selection]
                        edited_filter_value = st.multiselect("Select ASM Name(s)", options=asm_options, default=default_selection, key="edit_asm")
                    elif edited_role == "SO":
                        so_options = sorted(df['SO'].unique())
                        current_filter_index = so_options.index(edited_filter_value) if edited_filter_value in so_options else 0
                        edited_filter_value = st.selectbox("Select SO Name", options=so_options, index=current_filter_index, key="edit_so")
                    else:
                        edited_filter_value = None
                        st.write("No filter needed for ADMIN role.")

                if st.form_submit_button("Save Changes"):
                    user_dict[user_to_edit]["name"] = edited_name
                    user_dict[user_to_edit]["role"] = edited_role
                    user_dict[user_to_edit]["filter_value"] = edited_filter_value
                    if edited_password:
                        user_dict[user_to_edit]["password"] = hash_password(edited_password)
                    if save_credentials_to_ftp(credentials):
                        st.success(f"User '{user_to_edit}' updated successfully!")
                        st.rerun()

    with st.expander("➖ Delete User", expanded=False):
         with st.form("delete_form", clear_on_submit=True):
            user_to_delete = st.selectbox("Select User to Delete", options=[u for u in user_dict.keys() if u not in ["superadmin"]], key="delete_select")
            if st.form_submit_button("Delete User"):
                if user_to_delete in user_dict:
                    del user_dict[user_to_delete]
                    if save_credentials_to_ftp(credentials):
                        st.success(f"User '{user_to_delete}' deleted!")
                        st.rerun()

def main_dashboard_ui(df, user_role, user_filter_value):
    """This is the main dashboard UI that is visible to everyone."""
    
    if user_role == "RGM": df = df[df['RGM'] == user_filter_value].copy()
    elif user_role == "DSM": df = df[df['DSM'] == user_filter_value].copy()
    elif user_role == "ASM": df = df[df['ASM'].isin(user_filter_value)].copy()
    elif user_role == "SO": df = df[df['SO'] == user_filter_value].copy()
    
    if df.empty:
        st.warning(f"No data available in the last 45 days for your access level ('{user_filter_value}').")
        return

    st.sidebar.title("Filters")
    min_date, max_date = df['InvDate'].min().date(), df['InvDate'].max().date()
    start_date, end_date = st.sidebar.date_input("Select a Date Range", value=(max_date, max_date), min_value=min_date, max_value=max_date)
    
    df_hierarchical_filtered = df.copy()

    if user_role in ["SUPER_ADMIN", "ADMIN"]:
        if selected_rgm := st.sidebar.multiselect("Filter by RGM", sorted(df_hierarchical_filtered['RGM'].unique())): 
            df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['RGM'].isin(selected_rgm)]
    if user_role in ["SUPER_ADMIN", "ADMIN", "RGM"]:
        if selected_dsm := st.sidebar.multiselect("Filter by DSM", sorted(df_hierarchical_filtered['DSM'].unique())): 
            df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['DSM'].isin(selected_dsm)]
    if user_role in ["SUPER_ADMIN", "ADMIN", "RGM", "DSM"]:
        if selected_asm := st.sidebar.multiselect("Filter by ASM", sorted(df_hierarchical_filtered['ASM'].unique())): 
            df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['ASM'].isin(selected_asm)]
    if user_role in ["SUPER_ADMIN", "ADMIN", "RGM", "DSM", "ASM"]:
        if selected_cc := st.sidebar.multiselect("Filter by CustomerClass", sorted(df_hierarchical_filtered['CustomerClass'].unique())): 
            df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['CustomerClass'].isin(selected_cc)]
    
    if selected_so := st.sidebar.multiselect("Filter by SO", sorted(df_hierarchical_filtered['SO'].unique())): 
        df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['SO'].isin(selected_so)]

    df_filtered = df_hierarchical_filtered[(df_hierarchical_filtered['InvDate'].dt.date >= start_date) & (df_hierarchical_filtered['InvDate'].dt.date <= end_date)].copy()
    
    if df_filtered.empty:
        st.warning("No sales data available for the selected filters.")
        return

    st.markdown("---")
    st.header(f"Snapshot for {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
    summary_total_net_Volume = df_filtered['PrimaryQtyInLtrs/Kgs'].sum() / 1000
    summary_total_net_value = df_filtered['PrimaryLineTotalBeforeTax'].sum()
    summary_unique_invoices = df_filtered['InvNum'].nunique()
    summary_unique_dbs = df_filtered['BP Name'].nunique()
    Unique_prod_ctg = df_filtered['ProductCategory'].nunique()
    col1, col2, col3 = st.columns(3)
    col1.metric(label="Unique Prod Ctg", value=f"{Unique_prod_ctg}")
    col2.metric(label="Total Net Value", value=f"₹ {summary_total_net_value:,.0f}")
    col3.metric(label="Invoices Billed", value=f"{summary_unique_invoices}")
    col4, col5 = st.columns(2)
    col4.metric(label="Distributors Billed", value=f"{summary_unique_dbs}")
    col5.metric(label="Total Volume", value=f"{summary_total_net_Volume:,.2f}MT")
    st.markdown("---")
    st.header("Volume Comparison")
    single_kpi_date = end_date
    df_today = df_hierarchical_filtered[df_hierarchical_filtered['InvDate'].dt.date == single_kpi_date]
    todays_volume = df_today['PrimaryQtyInLtrs/Kgs'].sum() / 1000
    previous_day = single_kpi_date - timedelta(days=1)
    df_previous_day = df_hierarchical_filtered[df_hierarchical_filtered['InvDate'].dt.date == previous_day]
    yesterdays_volume = df_previous_day['PrimaryQtyInLtrs/Kgs'].sum() / 1000
    seven_day_start_date = single_kpi_date - timedelta(days=6)
    df_last_7_days = df_hierarchical_filtered[(df_hierarchical_filtered['InvDate'].dt.date >= seven_day_start_date) & (df_hierarchical_filtered['InvDate'].dt.date <= single_kpi_date)]
    past_7_days_volume = df_last_7_days['PrimaryQtyInLtrs/Kgs'].sum() / 1000
    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1: st.metric(label=f"End Date Volume ({single_kpi_date.strftime('%d-%b')})", value=f"{todays_volume:.2f} T")
    with kpi2: st.metric(label=f"Previous Day Volume ({previous_day.strftime('%d-%b')})", value=f"{yesterdays_volume:.2f} T")
    with kpi3: st.metric(label="Past 7 Days Volume", value=f"{past_7_days_volume:.2f} T", help=f"Total volume from {seven_day_start_date.strftime('%d-%b')} to {single_kpi_date.strftime('%d-%b')}")
    st.markdown("---")
    st.header("Detailed Performance View")
    
    all_options = ['Product Wise', 'Distributor Wise', 'DSM wise', 'ASM wise', 'ASE wise', 'SO Wise']
    
    if user_role in ["SUPER_ADMIN", "ADMIN"]:
        options_for_this_user = all_options
    elif user_role == "RGM":
        options_for_this_user = ['Product Wise', 'Distributor Wise', 'DSM wise', 'ASM wise', 'ASE wise', 'SO Wise']
    elif user_role == "DSM":
        options_for_this_user = ['Product Wise', 'Distributor Wise', 'ASE wise', 'SO Wise']
    elif user_role == "ASM":
        options_for_this_user = ['Product Wise', 'Distributor Wise', 'ASE wise', 'SO Wise']
    elif user_role == "SO":
        options_for_this_user = ['Product Wise', 'Distributor Wise', 'SO Wise']
    else:
        options_for_this_user = ['Product Wise', 'Distributor Wise']

    view_selection = st.radio(
        "Choose a view for the table below:",
        options_for_this_user,
        horizontal=True
    )
    if view_selection == 'Product Wise':
        st.subheader("Performance by Product Category")
        # --- CHANGE: Added 'upd_prod_ctg' to the groupby to show the new category ---
        group_cols = ['ProductCategory', 'upd_prod_ctg'] if 'upd_prod_ctg' in df_filtered.columns else ['ProductCategory']
        prod_ctg_performance = df_filtered.groupby(group_cols).agg(Total_Value=('PrimaryLineTotalBeforeTax', 'sum'), Total_Tonnes=('PrimaryQtyInLtrs/Kgs', lambda x: x.sum() / 1000), Distributors_Billed=('BP Name', 'nunique')).reset_index().sort_values('Total_Tonnes', ascending=False)
        prod_ctg_performance['Total_Value'] = prod_ctg_performance['Total_Value'].map('₹ {:,.0f}'.format)
        prod_ctg_performance['Total_Tonnes'] = prod_ctg_performance['Total_Tonnes'].map('{:.2f} T'.format)
        st.dataframe(prod_ctg_performance, use_container_width=True, hide_index=True)
    elif view_selection == 'Distributor Wise':
        st.subheader("Performance by Distributor")
        db_performance = df_filtered.groupby(['BP Code', 'BP Name']).agg(Total_Value=('PrimaryLineTotalBeforeTax', 'sum'), Total_Tonnes=('PrimaryQtyInLtrs/Kgs', lambda x: x.sum() / 1000), Unique_Products_Purchased_ct=('ProductCategory', 'nunique'), Unique_Products_Purchased=('ProductCategory','unique')).reset_index().sort_values('Total_Tonnes', ascending=False)
        db_performance['Total_Value'] = db_performance['Total_Value'].map('₹ {:,.0f}'.format)
        db_performance['Total_Tonnes'] = db_performance['Total_Tonnes'].map('{:.2f} T'.format)
        st.dataframe(db_performance, use_container_width=True, hide_index=True)
    elif view_selection == 'DSM wise':
        st.subheader("Performance by ASE")
        DSM_performance = df_filtered.groupby(['DSM']).agg(Total_Value=('PrimaryLineTotalBeforeTax', 'sum'), Total_Tonnes=('PrimaryQtyInLtrs/Kgs', lambda x: x.sum() / 1000), Distributors_Billed=('BP Code', 'unique'),Unique_Products_ct=('ProductCategory', 'nunique'), Unique_Products=('ProductCategory','unique')).reset_index().sort_values('Total_Tonnes', ascending=False)
        DSM_performance['Total_Value'] = DSM_performance['Total_Value'].map('₹ {:,.0f}'.format)
        DSM_performance['Total_Tonnes'] = DSM_performance['Total_Tonnes'].map('{:.2f} T'.format)
        st.dataframe(DSM_performance, use_container_width=True, hide_index=True)
    elif view_selection == 'ASE wise':
        st.subheader("Performance by ASE")
        ASE_performance = df_filtered.groupby(['ASM']).agg(Total_Value=('PrimaryLineTotalBeforeTax', 'sum'), Total_Tonnes=('PrimaryQtyInLtrs/Kgs', lambda x: x.sum() / 1000), Distributors_Billed=('BP Code', 'unique'),Unique_Products_ct=('ProductCategory', 'nunique'), Unique_Products=('ProductCategory','unique')).reset_index().sort_values('Total_Tonnes', ascending=False)
        ASE_performance['Total_Value'] = ASE_performance['Total_Value'].map('₹ {:,.0f}'.format)
        ASE_performance['Total_Tonnes'] = ASE_performance['Total_Tonnes'].map('{:.2f} T'.format)
        st.dataframe(ASE_performance, use_container_width=True, hide_index=True)
    
    elif view_selection == 'SO Wise':
        st.subheader("Performance by SO")
        SO_performance = df_filtered.groupby(['SO','ASM']).agg(Total_Value=('PrimaryLineTotalBeforeTax', 'sum'), Total_Tonnes=('PrimaryQtyInLtrs/Kgs', lambda x: x.sum() / 1000), Distributors_Billed=('BP Code', 'unique'),Unique_Products_ct=('ProductCategory', 'nunique'), Unique_Products=('ProductCategory','unique')).reset_index().sort_values('Total_Tonnes', ascending=False)
        SO_performance['Total_Value'] = SO_performance['Total_Value'].map('₹ {:,.0f}'.format)
        SO_performance['Total_Tonnes'] = SO_performance['Total_Tonnes'].map('{:.2f} T'.format)
        st.dataframe(SO_performance, use_container_width=True, hide_index=True)

# --- 5. AUTHENTICATION & PAGE ROUTING (NO CHANGE) ---

if "ftp" not in st.secrets:
    st.error("FTP credentials are not configured in Streamlit secrets. The app cannot start.")
    st.stop()

initialize_credentials_if_needed()
credentials = load_credentials_from_ftp()

if not credentials:
    st.error("Could not load credentials from FTP. App setup is incomplete.")
    st.stop()

authenticator = stauth.Authenticate(
    credentials['credentials'],
    credentials['cookie']['name'],
    credentials['cookie']['key'],
    credentials['cookie']['expiry_days']
)

st.title("Sales Performance Dashboard 📊")
authenticator.login()

if st.session_state["authentication_status"]:
    with st.sidebar:
        st.success(f'Welcome *{st.session_state["name"]}*')
        if st.sidebar.button("Refresh Data ❄️"):
            st.cache_data.clear()
            st.rerun()
        authenticator.logout('Logout', 'main')

    username = st.session_state["username"]
    user_details = credentials['credentials']['usernames'].get(username, {})
    user_role = user_details.get("role")
    user_filter_value = user_details.get("filter_value")
    
    start_timer = time.time()
    df_main, mod_time, error_message, status_message = load_main_data_from_ftp()
    end_timer = time.time()
    loading_time = end_timer - start_timer
    
    if error_message:
        st.error(error_message)
        st.stop()

    if status_message:
        st.toast(status_message, icon="⚡")
    
    if mod_time: 
        try:
            utc_time = datetime.strptime(mod_time, '%Y%m%d%H%M%S').replace(tzinfo=ZoneInfo("UTC"))
            ist_time = utc_time.astimezone(ZoneInfo("Asia/Kolkata"))
            formatted_time = ist_time.strftime("%d %b %Y, %I:%M:%S %p IST")
            st.caption(f"Data Last Refreshed: {formatted_time}")
        except Exception:
            st.caption(f"Dashboard Loaded: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}")
    else:
        st.caption(f"Dashboard Loaded: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}")
    st.caption(f"Dashboard loaded in {loading_time:.2f} seconds 🚀")

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
        st.error("Could not load dashboard data.")

elif st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')