import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import ftplib
from ftplib import FTP
import io
import os
from zoneinfo import ZoneInfo
import streamlit_authenticator as stauth
import json

# --- App Configuration (MUST be the first Streamlit command) ---
st.set_page_config(layout="wide", page_title="Sales Performance Dashboard")

# --- Function Definitions ---
# Ellame function-a define pannurom, aana ippo call pannala.

# @st.cache_data(ttl=300)  # <-- Intha cache-ah prachanai solve aana aprom enable pannikalam
def load_credentials_from_ftp():
    try:
        creds = st.secrets["ftp"]
        ftp = FTP(creds['host'])
        ftp.login(user=creds['user'], passwd=creds['password'])
        in_memory_file = io.BytesIO()
        ftp.retrbinary(f"RETR {creds['credentials_path']}", in_memory_file.write)
        in_memory_file.seek(0)
        ftp.quit()
        credentials = json.load(in_memory_file)
        return credentials
    except Exception as e:
        st.error(f"Error loading login credentials: {e}")
        return None

# @st.cache_data(ttl=300) # <-- Intha cache-ah yum prachanai solve aana aprom enable pannikalam
def load_data_from_ftp(_ftp_creds):
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
        common_columns = list(set(df_primary.columns).intersection(set(df_ctg.columns)))
        if not common_columns:
            st.error("Merge Error: No common column found.")
            return None, modification_time_str
        df = pd.merge(df_primary, df_ctg, on=common_columns[0], how='left')
        if 'Inv Date' not in df.columns:
            st.error("Data Error: 'Inv Date' column not found.")
            return None, modification_time_str
        df['Inv Date'] = pd.to_datetime(df['Inv Date'], format='%d-%b-%y', errors='coerce')
        df.dropna(subset=['Inv Date'], inplace=True)
        for col in ['Qty in Ltrs/Kgs', 'Net Value']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
        for col in ['ASM', 'RGM', 'DSM', 'SO', 'Prod Ctg', 'Cust Name', 'JCPeriod', 'CustomerClass']:
            if col in df.columns:
                df[col].fillna('Unknown', inplace=True)
        return df, modification_time_str
    except ftplib.all_errors as e:
        st.error(f"FTP Error: {e}")
        return None, None
    except Exception as e:
        st.error(f"An unexpected error occurred during data loading: {e}")
        return None, None

def run_dashboard(df, mod_time):
    # Intha function kulla unga mulu dashboard UI logic-um varum
    if mod_time:
        try:
            utc_time = datetime.strptime(mod_time, '%Y%m%d%H%M%S').replace(tzinfo=ZoneInfo("UTC"))
            ist_time = utc_time.astimezone(ZoneInfo("Asia/Kolkata"))
            formatted_time = ist_time.strftime("%d %b %Y, %I:%M:%S %p IST")
            st.caption(f"Data Last Refreshed: {formatted_time}")
        except Exception:
            st.caption("Could not determine data refresh time.")
    else:
        st.caption(f"Dashboard Loaded: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}")

    if df is not None:
        if df.empty:
            st.warning("No valid data found after cleaning.")
            st.stop()
        # --- SIDEBAR FILTERS ---
        st.sidebar.title("Filters")
        st.sidebar.header("Date Range Selection")
        min_date, max_date = df['Inv Date'].min().date(), df['Inv Date'].max().date()
        selected_date_range = st.sidebar.date_input("Select a Date Range", value=(max_date, max_date), min_value=min_date, max_date=max_date)
        if len(selected_date_range) != 2:
            st.stop()
        start_date, end_date = selected_date_range

        df_hierarchical_filtered = df.copy()
        for col in ['RGM', 'DSM', 'ASM', 'CustomerClass', 'SO']:
            if col in df.columns:
                options = sorted(df_hierarchical_filtered[col].unique())
                selected = st.sidebar.multiselect(f"Filter by {col}", options)
                if selected:
                    df_hierarchical_filtered = df_hierarchical_filtered[df_hierarchical_filtered[col].isin(selected)]

        df_filtered = df_hierarchical_filtered[(df_hierarchical_filtered['Inv Date'].dt.date >= start_date) & (df_hierarchical_filtered['Inv Date'].dt.date <= end_date)].copy()
        st.markdown("---")
        if df_filtered.empty:
            st.warning("No sales data available for the selected filters.")
            st.stop()

        # --- KPI CARDS ---
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

        # ... (Unga matha ella charts and tables logic inga varum)
        # --- Daily Volume Trend Analysis ---
        st.header("Volume Comparison")
        single_kpi_date = end_date
        todays_volume = df_hierarchical_filtered[df_hierarchical_filtered['Inv Date'].dt.date == single_kpi_date]['Qty in Ltrs/Kgs'].sum() / 1000
        previous_day = single_kpi_date - timedelta(days=1)
        yesterdays_volume = df_hierarchical_filtered[df_hierarchical_filtered['Inv Date'].dt.date == previous_day]['Qty in Ltrs/Kgs'].sum() / 1000
        seven_day_start_date = single_kpi_date - timedelta(days=6)
        past_7_days_volume = df_hierarchical_filtered[(df_hierarchical_filtered['Inv Date'].dt.date >= seven_day_start_date) & (df_hierarchical_filtered['Inv Date'].dt.date <= single_kpi_date)]['Qty in Ltrs/Kgs'].sum() / 1000
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric(label=f"End Date Volume ({single_kpi_date.strftime('%d-%b')})", value=f"{todays_volume:.2f} T")
        kpi2.metric(label=f"Previous Day Volume ({previous_day.strftime('%d-%b')})", value=f"{yesterdays_volume:.2f} T")
        kpi3.metric(label="Past 7 Days Volume", value=f"{past_7_days_volume:.2f} T")
        st.markdown("---")

        # --- DYNAMIC PERFORMANCE TABLE ---
        st.header("Detailed Performance View")
        view_selection = st.radio("Choose a view:", ['Product Wise', 'Distributor Wise'], horizontal=True)
        if view_selection == 'Product Wise':
            st.subheader("Performance by Product Category")
            # (Your Product Wise table logic)
            prod_ctg_performance = df_filtered.groupby('Prod Ctg').agg(Total_Value=('Net Value', 'sum'), Total_Tonnes=('Qty in Ltrs/Kgs', lambda x: x.sum() / 1000), Distributors_Billed=('Cust Name', 'nunique')).reset_index().sort_values('Total_Value', ascending=False)
            prod_ctg_performance['Total_Value'] = prod_ctg_performance['Total_Value'].map('₹ {:,.0f}'.format)
            prod_ctg_performance['Total_Tonnes'] = prod_ctg_performance['Total_Tonnes'].map('{:.2f} T'.format)
            st.dataframe(prod_ctg_performance, use_container_width=True)
        elif view_selection == 'Distributor Wise':
            st.subheader("Performance by Distributor")
            # (Your Distributor Wise table logic)
            db_performance = df_filtered.groupby(['Cust Name', 'City']).agg(Total_Value=('Net Value', 'sum'), Total_Tonnes=('Qty in Ltrs/Kgs', lambda x: x.sum() / 1000), Unique_Products_Purchased_ct=('Prod Ctg', 'nunique'), Unique_Products_Purchased=('Prod Ctg','unique')).reset_index().sort_values('Total_Value', ascending=False)
            db_performance['Total_Value'] = db_performance['Total_Value'].map('₹ {:,.0f}'.format)
            db_performance['Total_Tonnes'] = db_performance['Total_Tonnes'].map('{:.2f} T'.format)
            st.dataframe(db_performance, use_container_width=True)
    else:
        st.info("Data could not be loaded. Please check error messages.")

# --- Main function to control the app flow ---
def main():
    st.title("Sales Performance Dashboard 📊")
    
    credentials = load_credentials_from_ftp()

    if not credentials:
        st.error("Login system could not be initialized. Cannot proceed.")
        st.stop()

    cookie_config = {'name': "sales_dashboard_cookie", 'key': "a_secret_key", 'expiry_days': 30}
    authenticator = stauth.Authenticate(credentials, cookie_config['name'], cookie_config['key'], cookie_config['expiry_days'])

    with st.sidebar:
        authenticator.login()

    if st.session_state.get("authentication_status"):
        with st.sidebar:
            st.write(f'Welcome *{st.session_state["name"]}*')
            authenticator.logout('Logout', 'main')
        
        # Login aana aprom thaan, data-vai load panrom
        df, mod_time = load_data_from_ftp(st.secrets["ftp"])
        
        # Login aana aprom thaan, dashboard-a kaaturom
        run_dashboard(df, mod_time)

    elif st.session_state.get("authentication_status") is False:
        with st.sidebar:
            st.error('Username/password is incorrect')
    elif st.session_state.get("authentication_status") is None:
        with st.sidebar:
            st.warning('Please enter your username and password.')

# --- Script entry point ---
if __name__ == "__main__":
    main()
