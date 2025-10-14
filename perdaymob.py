import streamlit as st
import pandas as pd
import json
import os
import bcrypt # Password hash panna thevai
from datetime import datetime, timedelta
import ftplib
from ftplib import FTP
import io
from zoneinfo import ZoneInfo

# --- 1. SETUP AND HELPER FUNCTIONS ---
st.set_page_config(layout="wide", page_title="Sales Performance Dashboard")

def hash_password(password):
    """bcrypt use panni password-a secure hash-a maathum."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def save_credentials_to_ftp(credentials):
    """Pudhu user data-va credentials.json file-la FTP-la save pannum."""
    try:
        creds = st.secrets["ftp"]
        ftp = FTP(creds['host'])
        ftp.login(user=creds['user'], passwd=creds['password'])
        json_data = json.dumps(credentials, indent=2)
        in_memory_file = io.BytesIO(json_data.encode('utf-8'))
        ftp.storbinary(f"STOR {creds['credentials_path']}", in_memory_file)
        ftp.quit()
        return True
    except Exception as e:
        st.error(f"Could not save credentials to FTP: {e}")
        return False

# --- 2. DATA LOADING FUNCTIONS (FROM FTP) ---
@st.cache_data(ttl=300)
def load_credentials_from_ftp():
    """credentials.json file-la irundhu FTP-la irundhu user data-va load pannum."""
    try:
        creds = st.secrets["ftp"]
        ftp = FTP(creds['host'])
        ftp.login(user=creds['user'], passwd=creds['password'])
        in_memory_file = io.BytesIO()
        ftp.retrbinary(f"RETR {creds['credentials_path']}", in_memory_file.write)
        in_memory_file.seek(0)
        ftp.quit()
        return json.load(in_memory_file)
    except Exception as e:
        st.error(f"Error loading login credentials from FTP: {e}")
        return None

@st.cache_data(ttl=300)
def load_data_from_ftp(_ftp_creds):
    """Unga original data loading function."""
    modification_time_str = None
    try:
        def download_file_from_ftp(ftp, full_path):
            ftp.cwd("/")
            directory = os.path.dirname(full_path)
            filename = os.path.basename(full_path)
            ftp.cwd(directory)
            in_memory_file = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", in_memory_file.write)
            in_memory_file.seek(0)
            return in_memory_file
            
        ftp = FTP(_ftp_creds['host'])
        ftp.login(user=_ftp_creds['user'], passwd=_ftp_creds['password'])
        try:
            mdtm_response = ftp.sendcmd(f"MDTM {_ftp_creds['primary_path']}")
            modification_time_str = mdtm_response.split(' ')[1]
        except ftplib.all_errors: pass
        
        primary_file_obj = download_file_from_ftp(ftp, _ftp_creds['primary_path'])
        ctg_file_obj = download_file_from_ftp(ftp, _ftp_creds['category_path'])
        ftp.quit()
        
        df_primary = pd.read_csv(primary_file_obj, encoding='latin1', low_memory=False)
        df_ctg = pd.read_csv(ctg_file_obj, encoding='latin1', low_memory=False)
        
        primary_cols = set(df_primary.columns)
        ctg_cols = set(df_ctg.columns)
        common_columns = list(primary_cols.intersection(ctg_cols))
        if not common_columns:
            st.error("Merge Error: No common column found between data files.")
            return None, modification_time_str
            
        df = pd.merge(df_primary, df_ctg, on=common_columns[0], how='left')
        
        if 'Inv Date' not in df.columns:
            st.error("Data Error: 'Inv Date' column not found.")
            return None, modification_time_str
        df['Inv Date'] = pd.to_datetime(df['Inv Date'], format='%d-%b-%y', errors='coerce')
        df.dropna(subset=['Inv Date'], inplace=True)
        numeric_cols = ['Qty in Ltrs/Kgs', 'Net Value']
        for col in numeric_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
        key_cols = ['ASM', 'RGM', 'DSM', 'SO', 'Prod Ctg', 'Cust Name', 'JCPeriod', 'CustomerClass']
        for col in key_cols:
            if col in df.columns: df[col].fillna('Unknown', inplace=True)
        return df, modification_time_str
    except Exception as e:
        st.error(f"FTP Error while loading main data: {e}")
        return None, None

# --- 3. UI FUNCTIONS ---
def user_management_ui(credentials, df):
    """Super Admin-kaana User Management page."""
    st.subheader("👤 User Management")

    st.write("Existing Users:")
    users_data = [{"Username": u, "Name": d["name"], "Role": d["role"], "Filter Value": d.get("filter_value", "N/A")} for u, d in credentials["usernames"].items()]
    st.dataframe(pd.DataFrame(users_data), use_container_width=True)

    with st.expander("➕ Add New User", expanded=False):
        with st.form("add_user_form", clear_on_submit=True):
            st.write("Fill details to create a new user.")
            col1, col2, col3 = st.columns(3)
            with col1:
                new_username = st.text_input("Username").lower()
                new_name = st.text_input("Full Name")
            with col2:
                new_password = st.text_input("Password", type="password")
                new_role = st.selectbox("Role", ["ADMIN", "RGM", "DSM", "SO"], key="add_role")
            with col3:
                st.write("**Filter Value**")
                new_filter_value = None
                if df is not None:
                    if new_role == "RGM": new_filter_value = st.selectbox("Select RGM Name", options=sorted(df['RGM'].unique()), key="add_rgm", label_visibility="collapsed")
                    elif new_role == "DSM": new_filter_value = st.selectbox("Select DSM Name", options=sorted(df['DSM'].unique()), key="add_dsm", label_visibility="collapsed")
                    elif new_role == "SO": new_filter_value = st.selectbox("Select SO Name", options=sorted(df['SO'].unique()), key="add_so", label_visibility="collapsed")
                    else: st.text_input("Filter Value", "N/A for ADMIN", disabled=True)
                else: st.warning("Data not loaded, cannot show filter options.")
            if st.form_submit_button("Add User"):
                if not all([new_username, new_name, new_password, new_role]): st.error("Please fill all fields.")
                elif new_username in credentials["usernames"]: st.error(f"Username '{new_username}' already exists.")
                else:
                    credentials["usernames"][new_username] = {"name": new_name, "password": hash_password(new_password), "role": new_role, "filter_value": new_filter_value}
                    if save_credentials_to_ftp(credentials):
                        st.success(f"New user '{new_username}' added successfully!")
                        st.rerun()

    with st.expander("✏️ Edit Existing User", expanded=True):
        user_to_edit = st.selectbox("Select User to Edit", options=[u for u in credentials["usernames"].keys() if u != "superadmin"], index=None, placeholder="Choose a user...")
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
                    st.write("**Filter Value**")
                    edited_filter_value = user_data.get("filter_value")
                    if df is not None:
                        if edited_role == "RGM":
                            rgm_options = sorted(df['RGM'].unique())
                            current_filter_index = rgm_options.index(edited_filter_value) if edited_filter_value in rgm_options else 0
                            edited_filter_value = st.selectbox("Select RGM Name", options=rgm_options, index=current_filter_index, key="edit_rgm", label_visibility="collapsed")
                        elif edited_role == "DSM":
                            dsm_options = sorted(df['DSM'].unique())
                            current_filter_index = dsm_options.index(edited_filter_value) if edited_filter_value in dsm_options else 0
                            edited_filter_value = st.selectbox("Select DSM Name", options=dsm_options, index=current_filter_index, key="edit_dsm", label_visibility="collapsed")
                        elif edited_role == "SO":
                            so_options = sorted(df['SO'].unique())
                            current_filter_index = so_options.index(edited_filter_value) if edited_filter_value in so_options else 0
                            edited_filter_value = st.selectbox("Select SO Name", options=so_options, index=current_filter_index, key="edit_so", label_visibility="collapsed")
                        else:
                            edited_filter_value = None
                            st.text_input("Filter Value", "N/A for ADMIN", disabled=True, key="edit_filter_na")
                    else: st.warning("Data not loaded.")
                if st.form_submit_button("Save Changes"):
                    credentials["usernames"][user_to_edit].update({"name": edited_name, "role": edited_role, "filter_value": edited_filter_value})
                    if edited_password: credentials["usernames"][user_to_edit]["password"] = hash_password(edited_password)
                    if save_credentials_to_ftp(credentials):
                        st.success(f"User '{user_to_edit}' updated successfully!")
                        st.rerun()

    with st.expander("➖ Delete User", expanded=False):
         with st.form("delete_form", clear_on_submit=True):
            user_to_delete = st.selectbox("Select User to Delete", options=[u for u in credentials["usernames"].keys() if u not in ["superadmin"]], key="delete_select")
            if st.form_submit_button("Delete User"):
                if user_to_delete in credentials["usernames"]:
                    del credentials["usernames"][user_to_delete]
                    if save_credentials_to_ftp(credentials):
                        st.success(f"User '{user_to_delete}' deleted!")
                        st.rerun()

def main_dashboard_ui(df, mod_time, user_role, user_filter_value):
    st.title("Sales Performance Dashboard 📊")
    if mod_time:
        try:
            utc_time = datetime.strptime(mod_time, '%Y%m%d%H%M%S').replace(tzinfo=ZoneInfo("UTC"))
            ist_time = utc_time.astimezone(ZoneInfo("Asia/Kolkata"))
            st.caption(f"Data Last Refreshed: {ist_time.strftime('%d %b %Y, %I:%M:%S %p IST')}")
        except: st.caption("Could not determine data refresh time.")
    else: st.caption(f"Dashboard Loaded: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}")
    
    if user_role == "RGM": df = df[df['RGM'] == user_filter_value].copy()
    elif user_role == "DSM": df = df[df['DSM'] == user_filter_value].copy()
    elif user_role == "SO": df = df[df['SO'] == user_filter_value].copy()
    if df.empty:
        st.warning(f"No data available for your access level.")
        return
        
    # --- IDHU UNGA ORIGINAL SIDEBAR FILTERS ---
    st.sidebar.title("Filters")
    min_date, max_date = df['Inv Date'].min().date(), df['Inv Date'].max().date()
    start_date, end_date = st.sidebar.date_input("Select a Date Range", value=(max_date, max_date), min_value=min_date, max_value=max_date)
    
    df_hierarchical_filtered = df.copy()
    
    # Role-based hierarchy filters
    if user_role in ["SUPER_ADMIN", "ADMIN"]:
        if selected_rgm := st.sidebar.multiselect("Filter by RGM", sorted(df_hierarchical_filtered['RGM'].unique())): df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['RGM'].isin(selected_rgm)]
    if user_role in ["SUPER_ADMIN", "ADMIN", "RGM"]:
        if selected_dsm := st.sidebar.multiselect("Filter by DSM", sorted(df_hierarchical_filtered['DSM'].unique())): df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['DSM'].isin(selected_dsm)]
    if user_role in ["SUPER_ADMIN", "ADMIN", "RGM", "DSM"]:
        if selected_asm := st.sidebar.multiselect("Filter by ASM", sorted(df_hierarchical_filtered['ASM'].unique())): df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['ASM'].isin(selected_asm)]
        if selected_cc := st.sidebar.multiselect("Filter by CustomerClass", sorted(df_hierarchical_filtered['CustomerClass'].unique())): df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['CustomerClass'].isin(selected_cc)]
    
    if selected_so := st.sidebar.multiselect("Filter by SO", sorted(df_hierarchical_filtered['SO'].unique())): 
        df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['SO'].isin(selected_so)]

    df_filtered = df_hierarchical_filtered[(df_hierarchical_filtered['Inv Date'].dt.date >= start_date) & (df_hierarchical_filtered['Inv Date'].dt.date <= end_date)].copy()
    if df_filtered.empty:
        st.warning("No sales data for the selected filters.")
        return
        
    # --- IDHU UNGA FULL DASHBOARD UI ---
    st.markdown("---")
    st.header(f"Snapshot for {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
    summary_total_net_Volume = df_filtered['Qty in Ltrs/Kgs'].sum() / 1000
    summary_total_net_value = df_filtered['Net Value'].sum()
    summary_unique_invoices = df_filtered['Inv Num'].nunique()
    summary_unique_dbs = df_filtered['Cust Name'].nunique()
    Unique_prod_ctg = df_filtered['Prod Ctg'].nunique()
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
    df_today = df_hierarchical_filtered[df_hierarchical_filtered['Inv Date'].dt.date == single_kpi_date]
    todays_volume = df_today['Qty in Ltrs/Kgs'].sum() / 1000
    previous_day = single_kpi_date - timedelta(days=1)
    df_previous_day = df_hierarchical_filtered[df_hierarchical_filtered['Inv Date'].dt.date == previous_day]
    yesterdays_volume = df_previous_day['Qty in Ltrs/Kgs'].sum() / 1000
    seven_day_start_date = single_kpi_date - timedelta(days=6)
    df_last_7_days = df_hierarchical_filtered[(df_hierarchical_filtered['Inv Date'].dt.date >= seven_day_start_date) & (df_hierarchical_filtered['Inv Date'].dt.date <= single_kpi_date)]
    past_7_days_volume = df_last_7_days['Qty in Ltrs/Kgs'].sum() / 1000
    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1: st.metric(label=f"End Date Volume ({single_kpi_date.strftime('%d-%b')})", value=f"{todays_volume:.2f} T")
    with kpi2: st.metric(label=f"Previous Day Volume ({previous_day.strftime('%d-%b')})", value=f"{yesterdays_volume:.2f} T")
    with kpi3: st.metric(label="Past 7 Days Volume", value=f"{past_7_days_volume:.2f} T", help=f"Total volume from {seven_day_start_date.strftime('%d-%b')} to {single_kpi_date.strftime('%d-%b')}")
    st.markdown("---")
    st.header("Detailed Performance View")
    view_selection = st.radio("Choose a view for the table below:", ['Product Wise', 'Distributor Wise'], horizontal=True)
    if view_selection == 'Product Wise':
        st.subheader("Performance by Product Category")
        prod_ctg_performance = df_filtered.groupby('Prod Ctg').agg(Total_Value=('Net Value', 'sum'), Total_Tonnes=('Qty in Ltrs/Kgs', lambda x: x.sum() / 1000), Distributors_Billed=('Cust Name', 'nunique')).reset_index().sort_values('Total_Value', ascending=False)
        prod_ctg_performance['Total_Value'] = prod_ctg_performance['Total_Value'].map('₹ {:,.0f}'.format)
        prod_ctg_performance['Total_Tonnes'] = prod_ctg_performance['Total_Tonnes'].map('{:.2f} T'.format)
        st.dataframe(prod_ctg_performance, use_container_width=True)
    elif view_selection == 'Distributor Wise':
        st.subheader("Performance by Distributor")
        db_performance = df_filtered.groupby(['Cust Name', 'City']).agg(Total_Value=('Net Value', 'sum'), Total_Tonnes=('Qty in Ltrs/Kgs', lambda x: x.sum() / 1000), Unique_Products_Purchased_ct=('Prod Ctg', 'nunique'), Unique_Products_Purchased=('Prod Ctg','unique')).reset_index().sort_values('Total_Value', ascending=False)
        db_performance['Total_Value'] = db_performance['Total_Value'].map('₹ {:,.0f}'.format)
        db_performance['Total_Tonnes'] = db_performance['Total_Tonnes'].map('{:.2f} T'.format)
        st.dataframe(db_performance, use_container_width=True)


# --- 4. AUTHENTICATION AND PAGE ROUTING ---
credentials = load_credentials_from_ftp()

if 'authentication_status' not in st.session_state: st.session_state['authentication_status'] = None
if 'username' not in st.session_state: st.session_state['username'] = None

if not st.session_state['authentication_status']:
    st.title("Sales Performance Dashboard 📊")
    with st.sidebar:
        st.header("Login")
        login_username = st.text_input("Username").lower()
        login_password = st.text_input("Password", type="password")
        if st.button("Login"):
            if credentials and login_username in credentials["usernames"]:
                stored_hash = credentials["usernames"][login_username]["password"].encode()
                if bcrypt.checkpw(login_password.encode(), stored_hash):
                    st.session_state['authentication_status'] = True
                    st.session_state['username'] = login_username
                    st.rerun()
                else: st.session_state['authentication_status'] = False
            else: st.session_state['authentication_status'] = False
            if st.session_state['authentication_status'] is False: st.error("Username/password is incorrect.")
    st.info("Please login to access the dashboard.")

else: # Login Successful
    username = st.session_state['username']
    user_details = credentials["usernames"].get(username, {})
    user_role = user_details.get("role")
    user_filter_value = user_details.get("filter_value")
    name = user_details.get("name")
    
    with st.sidebar:
        st.success(f"Welcome *{name}* ({user_role})")
        if st.button("Logout"):
            st.session_state.clear()
            st.rerun()

    df_main, mod_time = load_main_data_from_ftp(st.secrets["ftp"])
    
    if df_main is not None:
        if user_role == "SUPER_ADMIN":
            page = st.sidebar.radio("Navigation", ["Dashboard", "User Management"])
            if page == "Dashboard":
                main_dashboard_ui(df_main, mod_time, user_role, user_filter_value)
            elif page == "User Management":
                user_management_ui(credentials, df_main)
        else:
            main_dashboard_ui(df_main, mod_time, user_role, user_filter_value)
    else:
        st.error("Failed to load main dashboard data. Please check FTP connection and file paths.")