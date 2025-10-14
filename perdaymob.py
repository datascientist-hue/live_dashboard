import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import ftplib
from ftplib import FTP
import io
import os
from zoneinfo import ZoneInfo
import streamlit_authenticator as stauth # LOGIN SYSTEM-KAAGA

# --- App Configuration & Title ---
st.set_page_config(layout="wide", page_title="Sales Performance Dashboard")
st.title("Sales Performance Dashboard 📊")

# --- STEP 1: LOGIN SYSTEM SETUP ---

# Function to load user details securely from your FTP server
@st.cache_data(ttl=300) # Cache user details for 5 minutes
def load_credentials_from_ftp():
    try:
        creds = st.secrets["ftp"]
        ftp = FTP(creds['host'])
        ftp.login(user=creds['user'], passwd=creds['password'])
        
        in_memory_file = io.BytesIO()
        # Make sure 'credentials_path' is set in your Streamlit secrets
        ftp.retrbinary(f"RETR {creds['credentials_path']}", in_memory_file.write)
        in_memory_file.seek(0)
        ftp.quit()
        
        df_creds = pd.read_csv(in_memory_file)
        
        # Convert the CSV data into the format required by the authenticator
        credentials = {
            "usernames": {
                row['username']: {
                    "name": row['name'],
                    "password": row['password']
                }
                for index, row in df_creds.iterrows()
            }
        }
        return credentials
    except Exception as e:
        st.error(f"Error loading login credentials: {e}")
        return None

# Load the credentials from the function
credentials = load_credentials_from_ftp()

# Cookie configuration for session management
cookie_config = {
    'name': "sales_dashboard_cookie",
    'key': "a_secret_key_for_cookie_signature", # You can change this key to anything
    'expiry_days': 30
}

# Initialize the authenticator
if credentials:
    authenticator = stauth.Authenticate(
        credentials,
        cookie_config['name'],
        cookie_config['key'],
        cookie_config['expiry_days'],
    )
    # Render the login form in the sidebar
    with st.sidebar:
        name, authentication_status, username = authenticator.login('Login', 'main')
else:
    # If credentials cannot be loaded, stop the app
    st.error("Login system could not be initialized. Please contact the administrator.")
    st.stop()

# --- STEP 2: CHECK LOGIN STATUS ---
# The rest of your app will only run if authentication_status is True (successful login)

if st.session_state["authentication_status"]:
    
    # Add a logout button to the sidebar after login
    with st.sidebar:
        st.write(f'Welcome *{name}*')
        authenticator.logout('Logout', 'main')

    # --- UNGA EXISTING CODE INGA IRUNDHU START AAGUDHU ---
    # (Endha maatharamum seiyapadavillai)

    # --- DATA LOADING AND CLEANING ---
    @st.cache_data(ttl=300)
    def load_data_from_ftp(_ftp_creds):
        """
        Securely loads data and fetches the modification time of the primary file from an FTP server.
        """
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
            except ftplib.all_errors:
                pass 

            primary_file_obj = download_file_from_ftp(ftp, _ftp_creds['primary_path'])
            ctg_file_obj = download_file_from_ftp(ftp, _ftp_creds['category_path'])

            ftp.quit()

            df_primary = pd.read_csv(primary_file_obj, encoding='latin1', low_memory=False)
            df_ctg = pd.read_csv(ctg_file_obj, encoding='latin1', low_memory=False)

            primary_cols = set(df_primary.columns)
            ctg_cols = set(df_ctg.columns)
            common_columns = list(primary_cols.intersection(ctg_cols))

            if not common_columns:
                st.error("Merge Error: No common column found between the primary and category files.")
                return None, modification_time_str

            merge_on_column = common_columns[0]
            df = pd.merge(df_primary, df_ctg, on=merge_on_column, how='left')

            if 'Inv Date' not in df.columns:
                st.error("Data Error: The column 'Inv Date' was not found.")
                return None, modification_time_str

            df['Inv Date'] = pd.to_datetime(df['Inv Date'], format='%d-%b-%y', errors='coerce')

            if df['Inv Date'].isnull().all() and len(df) > 0:
                st.error("CRITICAL DATA ERROR: All rows have an invalid date format.")
                return None, modification_time_str

            df.dropna(subset=['Inv Date'], inplace=True)

            numeric_cols = ['Qty in Ltrs/Kgs', 'Net Value']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

            key_cols = ['ASM', 'RGM', 'DSM', 'SO', 'Prod Ctg', 'Cust Name', 'JCPeriod', 'CustomerClass']
            for col in key_cols:
                if col in df.columns:
                    df[col].fillna('Unknown', inplace=True)

            return df, modification_time_str

        except ftplib.all_errors as e:
            st.error(f"FTP Error: Could not connect or find the file. Details: {e}")
            return None, None
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
            return None, None

    # --- Fetch data and display the file's modification time in IST ---
    df, mod_time = load_data_from_ftp(st.secrets["ftp"])

    if mod_time:
        try:
            utc_time = datetime.strptime(mod_time, '%Y%m%d%H%M%S')
            utc_time = utc_time.replace(tzinfo=ZoneInfo("UTC"))
            ist_time = utc_time.astimezone(ZoneInfo("Asia/Kolkata"))
            formatted_time = ist_time.strftime("%d %b %Y, %I:%M:%S %p IST")
            st.caption(f"Data Last Refreshed: {formatted_time}")
            
        except Exception:
            st.caption("Could not determine the exact data refresh time.")
    else:
        st.caption(f"Dashboard Loaded: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}")

    # --- MAIN DASHBOARD LOGIC ---
    if df is not None:

        if df.empty:
            st.warning("No valid sales data was found after the cleaning process.")
            st.stop()

        # --- ADVANCED SIDEBAR ---
        # Note: The sidebar is now used for both login and filters.
        # We add a title for the filter section.
        st.sidebar.title("Filters")

        # --- DATE RANGE FILTER ---
        st.sidebar.header("Date Range Selection")

        min_date = df['Inv Date'].min().date()
        max_date = df['Inv Date'].max().date()

        selected_date_range = st.sidebar.date_input(
            "Select a Date Range",
            value=(max_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        if len(selected_date_range) != 2:
            st.sidebar.warning("Please select a valid start and end date.")
            st.stop()

        start_date, end_date = selected_date_range

        # --- HIERARCHICAL FILTERS ---
        df_hierarchical_filtered = df.copy()

        if 'RGM' in df.columns:
            rgm_options = sorted(df['RGM'].unique())
            selected_rgm = st.sidebar.multiselect("Filter by RGM", rgm_options)
            if selected_rgm:
                df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['RGM'].isin(selected_rgm)]

        if 'DSM' in df.columns:
            dsm_options = sorted(df_hierarchical_filtered['DSM'].unique())
            selected_dsm = st.sidebar.multiselect("Filter by DSM", dsm_options)
            if selected_dsm:
                df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['DSM'].isin(selected_dsm)]

        if 'ASM' in df.columns:
            asm_options = sorted(df_hierarchical_filtered['ASM'].unique())
            selected_asm = st.sidebar.multiselect("Filter by ASM", asm_options)
            if selected_asm:
                df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['ASM'].isin(selected_asm)]

        if 'CustomerClass' in df.columns:
            CustomerClass_option = sorted(df_hierarchical_filtered['CustomerClass'].unique())
            selected_CustomerClass = st.sidebar.multiselect("Filter by CustomerClass", CustomerClass_option)
            if selected_CustomerClass:
                df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['CustomerClass'].isin(selected_CustomerClass)]

        if 'SO' in df.columns:
            SO_option = sorted(df_hierarchical_filtered['SO'].unique())
            selected_SO = st.sidebar.multiselect("Filter by SO", SO_option)
            if selected_SO:
                df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered['SO'].isin(selected_SO)]

        df_filtered = df_hierarchical_filtered[
            (df_hierarchical_filtered['Inv Date'].dt.date >= start_date) &
            (df_hierarchical_filtered['Inv Date'].dt.date <= end_date)
        ].copy()

        st.markdown("---")

        if df_filtered.empty:
            st.warning("No sales data available for the combination of selected filters.")
            st.stop()

        # --- TOP ROW SUMMARY KPI CARDS ---
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

        # --- Daily Volume Trend Analysis ---
        st.header("Volume Comparison")

        single_kpi_date = end_date

        df_today = df_hierarchical_filtered[df_hierarchical_filtered['Inv Date'].dt.date == single_kpi_date]
        todays_volume = df_today['Qty in Ltrs/Kgs'].sum() / 1000

        previous_day = single_kpi_date - timedelta(days=1)
        df_previous_day = df_hierarchical_filtered[df_hierarchical_filtered['Inv Date'].dt.date == previous_day]
        yesterdays_volume = df_previous_day['Qty in Ltrs/Kgs'].sum() / 1000

        seven_day_start_date = single_kpi_date - timedelta(days=6)
        df_last_7_days = df_hierarchical_filtered[
            (df_hierarchical_filtered['Inv Date'].dt.date >= seven_day_start_date) &
            (df_hierarchical_filtered['Inv Date'].dt.date <= single_kpi_date)
        ]
        past_7_days_volume = df_last_7_days['Qty in Ltrs/Kgs'].sum() / 1000

        kpi1, kpi2, kpi3 = st.columns(3)

        with kpi1:
            st.metric(
                label=f"End Date Volume ({single_kpi_date.strftime('%d-%b')})",
                value=f"{todays_volume:.2f} T"
            )

        with kpi2:
            st.metric(
                label=f"Previous Day Volume ({previous_day.strftime('%d-%b')})",
                value=f"{yesterdays_volume:.2f} T"
            )

        with kpi3:
            st.metric(
                label="Past 7 Days Volume",
                value=f"{past_7_days_volume:.2f} T",
                help=f"Total volume from {seven_day_start_date.strftime('%d-%b')} to {single_kpi_date.strftime('%d-%b')}"
            )
        st.markdown("---")

        # --- DYNAMIC PERFORMANCE TABLE ---
        st.header("Detailed Performance View")

        view_selection = st.radio(
            label="Choose a view for the table below:",
            options=['Product Wise', 'Distributor Wise'],
            horizontal=True,
        )

        if view_selection == 'Product Wise':
            st.subheader("Performance by Product Category")
            prod_ctg_performance = df_filtered.groupby('Prod Ctg').agg(
                Total_Value=('Net Value', 'sum'),
                Total_Tonnes=('Qty in Ltrs/Kgs', lambda x: x.sum() / 1000),
                Distributors_Billed=('Cust Name', 'nunique')
            ).reset_index().sort_values('Total_Value', ascending=False)

            prod_ctg_performance['Total_Value'] = prod_ctg_performance['Total_Value'].map('₹ {:,.0f}'.format)
            prod_ctg_performance['Total_Tonnes'] = prod_ctg_performance['Total_Tonnes'].map('{:.2f} T'.format)

            st.dataframe(prod_ctg_performance, use_container_width=True)

        elif view_selection == 'Distributor Wise':
            st.subheader("Performance by Distributor")
            db_performance = df_filtered.groupby(['Cust Name', 'City']).agg(
                Total_Value=('Net Value', 'sum'),
                Total_Tonnes=('Qty in Ltrs/Kgs', lambda x: x.sum() / 1000),
                Unique_Products_Purchased_ct=('Prod Ctg', 'nunique'),
                Unique_Products_Purchased=('Prod Ctg','unique')
            ).reset_index().sort_values('Total_Value', ascending=False)

            db_performance['Total_Value'] = db_performance['Total_Value'].map('₹ {:,.0f}'.format)
            db_performance['Total_Tonnes'] = db_performance['Total_Tonnes'].map('{:.2f} T'.format)

            st.dataframe(db_performance, use_container_width=True)

    else:
        st.info("Data could not be loaded or processed. Please check the error messages above for details.")

# --- STEP 3: HANDLE LOGIN ERRORS ---
elif st.session_state["authentication_status"] is False:
    with st.sidebar:
        st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    with st.sidebar:
        st.warning('Please enter your username and password to access the dashboard.')