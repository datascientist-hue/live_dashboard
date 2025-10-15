# app.py
import streamlit as st
import pandas as pd
import json
import os
import bcrypt  # Password hash panna thevai
from datetime import datetime, timedelta
import ftplib
from ftplib import FTP  # FTP server-udan inaiya thevai
import io  # Memory-il file-kalai kaiyala thevai

# --- 1. APP CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Sales Dashboard")


# --- 2. FTP-BASED HELPER FUNCTIONS FOR USER MANAGEMENT ---

def load_credentials_from_ftp():
    """credentials.json file-la irundhu user data-va FTP-la irundhu load pannum."""
    try:
        creds = st.secrets["ftp"]
        ftp = FTP(creds['host'])
        ftp.login(user=creds['user'], passwd=creds['password'])
        
        # File-a memory-la download seiyavum
        in_memory_file = io.BytesIO()
        ftp.retrbinary(f"RETR {creds['credentials_path']}", in_memory_file.write)
        in_memory_file.seek(0)  # Buffer-in aarambathirku sellavum
        ftp.quit()
        
        return json.load(in_memory_file)
        
    except ftplib.error_perm:
        # File illai endral, idhu varum. Idhu oru thavaru alla, mudhal murai setup seiyum podhu nigazhum.
        return None
    except Exception as e:
        st.error(f"FTP Error: Login credentials-a load panna mudiyavillai: {e}")
        return None

def save_credentials_to_ftp(credentials):
    """Pudhu user data-va credentials.json file-la FTP-la save pannum."""
    try:
        creds = st.secrets["ftp"]
        ftp = FTP(creds['host'])
        ftp.login(user=creds['user'], passwd=creds['password'])
        
        # JSON data-va memory buffer-il ezhudhavum
        json_data = json.dumps(credentials, indent=4)
        in_memory_file = io.BytesIO(json_data.encode('utf-8'))
        
        # Memory-il irundhu FTP-ku file-a upload seiyavum
        ftp.storbinary(f"STOR {creds['credentials_path']}", in_memory_file)
        ftp.quit()
        return True
    except Exception as e:
        st.error(f"FTP Error: Credentials-a save panna mudiyavillai: {e}")
        return False

def hash_password(password):
    """bcrypt use panni password-a secure hash-a maathum."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def initialize_credentials_if_needed():
    """
    FTP-la credentials.json file illana, Streamlit secrets-la irundhu
    default superadmin user-oda onna create pannum.
    """
    if load_credentials_from_ftp() is None:
        st.warning("`credentials.json` FTP-il kanapadavillai. Oru pudhiya file Super Admin-udan uruvaakkappadugiradhu.")
        
        try:
            # Secrets-il irundhu paadhukaappaana password-a edukkavum
            initial_admin_pass = st.secrets["initial_admin"]["password"]
        except (KeyError, AttributeError):
            st.error("FATAL ERROR: `initial_admin` password Streamlit secrets-il configure seiyyappadavillai. App-a thodanga mudiyadhu.")
            st.stop()

        default_credentials = {
            "usernames": {
                "superadmin": {
                    "name": "Super Admin",
                    "password": hash_password(initial_admin_pass), # Secrets-il irundhu hash seiyappatta paadhukaappaana password
                    "role": "SUPER_ADMIN",
                    "filter_value": None
                }
            }
        }
        
        if save_credentials_to_ftp(default_credentials):
            st.success("Default Super Admin uruvaakkappattadhu. Login seiyavum.")
            st.rerun()
        else:
            st.error("FATAL ERROR: Credentials file-a FTP-il uruvaakka mudiyavillai.")
            st.stop()


# --- 3. FTP-BASED DATA LOADING FUNCTION ---
@st.cache_data(ttl=300) # 5 nimidangalukku data-vai cache seiyavum
def load_main_data_from_ftp():
    """primary.csv file-a FTP-la irundhu load seithu, clean seiyum."""
    try:
        ftp_creds = st.secrets["ftp"]
        ftp = FTP(ftp_creds['host'])
        ftp.login(user=ftp_creds['user'], passwd=ftp_creds['password'])

        # primary.csv file-a memory-la download seiyavum
        in_memory_file = io.BytesIO()
        ftp.retrbinary(f"RETR {ftp_creds['primary_path']}", in_memory_file.write)
        in_memory_file.seek(0)
        ftp.quit()

        df = pd.read_csv(in_memory_file, encoding='latin1', low_memory=False)
        
        # Unga original data cleaning steps ( எந்த மாற்றமும் இல்லை )
        if 'Inv Date' not in df.columns:
            st.error("Data Error: The column 'Inv Date' was not found.")
            return None
        df['Inv Date'] = pd.to_datetime(df['Inv Date'], format='%d-%b-%y', errors='coerce')
        df.dropna(subset=['Inv Date'], inplace=True)
        numeric_cols = ['Qty in Ltrs/Kgs', 'Net Value']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
        key_cols = ['ASM', 'RGM', 'DSM', 'SO', 'Prod Ctg', 'Cust Name', 'JCPeriod', 'CustomerClass']
        for col in key_cols:
            if col in df.columns:
                df[col].fillna('Unknown', inplace=True)
        return df
    except ftplib.all_errors as e:
        st.error(f"FTP Error: Data file-a kandupudikka mudiyavillai. Path sari paarkavum. Vivaram: {e}")
        return None
    except Exception as e:
        st.error(f"Error loading main data: {e}")
        return None

# --- 4. UI FUNCTIONS ( எந்த மாற்றமும் இல்லை ) ---

def user_management_ui(credentials, df):
    """Super Admin-kaana User Management page - UPDATED with separate Add and Edit forms."""
    st.subheader("👤 User Management")

    st.write("Existing Users:")
    users_data = [{"Username": u, "Name": d["name"], "Role": d["role"], "Filter Value": d.get("filter_value", "N/A")} for u, d in credentials["usernames"].items()]
    st.dataframe(pd.DataFrame(users_data), use_container_width=True)

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
                    role_options = ["ADMIN", "RGM", "DSM", "ASM" ,"SO"]
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
                    elif edited_role == "ASM":
                        dsm_options = sorted(df['ASM'].unique())
                        current_filter_index = asm_options.index(edited_filter_value) if edited_filter_value in asm_options else 0
                        edited_filter_value = st.selectbox("Select ASM Name", options=asm_options, index=current_filter_index, key="edit_asm")
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
    """Ellarukum theriyura main dashboard - IDHU THAAN UNGA FULL DASHBOARD UI."""
    st.title("Sales Performance Dashboard 📊")
    st.caption(f"Dashboard Loaded: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}")

    if user_role == "RGM": df = df[df['RGM'] == user_filter_value].copy()
    elif user_role == "DSM": df = df[df['DSM'] == user_filter_value].copy()
    elif user_role == "ASM": df = df[df['ASM'] == user_filter_value].copy()
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
    if user_role in ["SUPER_ADMIN", "ADMIN", "RGM"]:
        if selected_dsm := st.sidebar.multiselect("Filter by DSM", sorted(df_hierarchical_filtered['DSM'].unique())): 
            df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['DSM'].isin(selected_dsm)]
    if user_role in ["SUPER_ADMIN", "ADMIN", "RGM", "DSM", "ASM"]:
        if selected_asm := st.sidebar.multiselect("Filter by ASM", sorted(df_hierarchical_filtered['ASM'].unique())): 
            df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['ASM'].isin(selected_asm)]
        if selected_cc := st.sidebar.multiselect("Filter by CustomerClass", sorted(df_hierarchical_filtered['CustomerClass'].unique())): 
            df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['CustomerClass'].isin(selected_cc)]
    
    if selected_so := st.sidebar.multiselect("Filter by SO", sorted(df_hierarchical_filtered['SO'].unique())): 
        df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['SO'].isin(selected_so)]

    df_filtered = df_hierarchical_filtered[(df_hierarchical_filtered['Inv Date'].dt.date >= start_date) & (df_hierarchical_filtered['Inv Date'].dt.date <= end_date)].copy()
    
    if df_filtered.empty:
        st.warning("No sales data available for the selected filters.")
        return

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


# --- 5. AUTHENTICATION & PAGE ROUTING (MODIFIED FOR FTP) ---

# App thodangum mun, secrets configure seiyappattulladha enru sari paarkavum
if "ftp" not in st.secrets:
    st.error("FTP credentials Streamlit secrets-il configure seiyyappadavillai. App-a thodanga mudiyadhu.")
    st.stop()

# Mudhalil credentials file-a uruvaakka/sari paarkka vendum
initialize_credentials_if_needed()
credentials = load_credentials_from_ftp()

# Credentials load seiyya mudiyavillai endral app-a niruthavum
if not credentials:
    st.error("FTP-il irundhu credentials-a load seiyya mudiyavillai. App setup sari illai.")
    st.stop()

# Session state-a initialize seiyavum
if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] = None
if 'username' not in st.session_state:
    st.session_state['username'] = None

# Login seiyyaathavarukku login form-a kaattavum
if not st.session_state['authentication_status']:
    with st.sidebar:
        st.header("Login")
        login_username = st.text_input("Username").lower()
        login_password = st.text_input("Password", type="password")
        if st.button("Login"):
            if login_username in credentials["usernames"]:
                stored_hash = credentials["usernames"][login_username]["password"].encode()
                if bcrypt.checkpw(login_password.encode(), stored_hash):
                    st.session_state['authentication_status'] = True
                    st.session_state['username'] = login_username
                    st.rerun()
                else:
                    st.session_state['authentication_status'] = False
            else:
                st.session_state['authentication_status'] = False
            
            if st.session_state['authentication_status'] is False:
                st.error("Username/password is incorrect.")
    st.info("Please login to access the dashboard.")

# Login seithavarukku dashboard-a kaattavum
else:
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

    # Login vetrigaramaaga aanapin, main data-va load seiyavum
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
        st.error("Could not load dashboard data.")