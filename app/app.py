import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import requests
import os

# ---------------
# Load Data
# ---------------
@st.cache_data
def load_data():
    dropbox_url = "https://www.dropbox.com/scl/fi/2wcl5m09a62yy1myvect7/dummy_air_quality.duckdb?rlkey=n1d4iohcx0qlt3z2pwm8l7osv&st=4nzlsvar&dl=1"

    local_path = "/tmp/dummy_air_quality.duckdb"

    # Download from Dropbox if not already downloaded
    if not os.path.exists(local_path):
        st.info("Downloading database from Dropbox...")
        response = requests.get(dropbox_url)
        with open(local_path, "wb") as f:
            f.write(response.content)
        st.success("Download complete.")

    conn = duckdb.connect(local_path, read_only=True)
    df_hourly = conn.execute("SELECT * FROM air_quality_hourly").fetchdf()
    conn.close()
    return df_hourly

df = load_data()
df['Hour_Timestamp'] = pd.to_datetime(df['Hour_Timestamp'])
zip_codes = sorted(df["Zip_Code"].unique())

# ---------------
# Sidebar Filters
# ---------------
st.sidebar.header("🔎 Filters")

# ZIP filter
with st.sidebar.expander("Select ZIP Codes", expanded=True):
    zip_checks = {z: st.checkbox(z, value=True) for z in zip_codes}
    selected_zips = [z for z, checked in zip_checks.items() if checked]

# Date filter
min_date = df["Hour_Timestamp"].min().date()
max_date = df["Hour_Timestamp"].max().date()

date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date],
                                   min_value=min_date, max_value=max_date)

# Apply filters
filtered_df = df[
    (df["Zip_Code"].isin(selected_zips)) &
    (df["Hour_Timestamp"].dt.date >= date_range[0]) &
    (df["Hour_Timestamp"].dt.date <= date_range[1])
]

# ---------------
# Main Layout: Tabs
# ---------------
st.title("🌫 FHA - Air Quality Dashboard")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Trends", "🗺 Map", "ℹ️ About"])

# ---------------
# Overview Tab
# ---------------
with tab1:
    st.header("Air Quality Summary")

    # ---------------- Global ZIP Code Filter ----------------
    with st.expander("Filter ZIP Codes", expanded=False):
        if "selected_zips" not in st.session_state:
            st.session_state.selected_zips = zip_codes.copy()

        selected_zips = st.multiselect(
            "ZIP Codes:", zip_codes,
            default=st.session_state.selected_zips,
            key="zip_filter"
        )

        if st.button("Reset ZIPs"):
            selected_zips = zip_codes
            st.session_state.selected_zips = zip_codes.copy()

    # ---------------- Global Cascading Date Filter ----------------
    with st.expander("Filter Date Period", expanded=False):
        # Generate all Year-Month pairs available in data
        df_dates = df["Hour_Timestamp"].dt.to_period("M").drop_duplicates().sort_values()
        year_month_pairs = [(p.year, p.month) for p in df_dates]

        # Build year options
        years = sorted(set(y for y, m in year_month_pairs))

        # Use session_state to preserve selection across reruns
        if "selected_year_start" not in st.session_state:
            st.session_state.selected_year_start = years[0]
            st.session_state.selected_year_end = years[-1]

        # Year selection
        col1, col2 = st.columns(2)
        selected_year_start = col1.selectbox("Start Year", years, index=years.index(st.session_state.selected_year_start))
        selected_year_end = col2.selectbox("End Year", years, index=years.index(st.session_state.selected_year_end))

        # Month options depend on selected year
        months_lookup = {
            year: sorted(m for y, m in year_month_pairs if y == year) for year in years
        }

        # Default months
        default_start_month = months_lookup[selected_year_start][0]
        default_end_month = months_lookup[selected_year_end][-1]

        # Month selection
        col3, col4 = st.columns(2)
        selected_month_start = col3.selectbox(
            "Start Month", 
            [datetime(1900, m, 1).strftime('%B') for m in months_lookup[selected_year_start]],
            index=0
        )
        selected_month_end = col4.selectbox(
            "End Month",
            [datetime(1900, m, 1).strftime('%B') for m in months_lookup[selected_year_end]],
            index=len(months_lookup[selected_year_end])-1
        )

        if st.button("Reset Period"):
            selected_year_start = years[0]
            selected_year_end = years[-1]

        # Convert final selection to datetime objects
        start_dt = datetime.strptime(f"{selected_month_start} {selected_year_start}", "%B %Y")
        end_dt = datetime.strptime(f"{selected_month_end} {selected_year_end}", "%B %Y")
        end_dt = end_dt.replace(day=1) + pd.offsets.MonthEnd(1)

    # ---------------- Apply Both Filters ----------------
    filtered_df = df[
        (df["Zip_Code"].isin(selected_zips)) &
        (df["Hour_Timestamp"] >= start_dt) &
        (df["Hour_Timestamp"] <= end_dt)
    ]

    # Sub-header showing selected period
    st.markdown(
        f"<h5 style='color: grey; margin-top: -10px;'>({start_dt.strftime('%b %Y')} - {end_dt.strftime('%b %Y')})</h5>",
        unsafe_allow_html=True
    )

    # ---------------- Sub-Tabs ----------------
    subtab1, subtab2 = st.tabs(["🔢 Summary Metrics", "🎯 AQI Categories"])

    # -------- Summary Metrics --------
    with subtab1:
        if filtered_df.empty:
            st.warning("No data available for selected filters.")
        else:
            latest = filtered_df.sort_values("Hour_Timestamp").groupby("Zip_Code").tail(1)
            avg_aqi = round(filtered_df["Avg_AQI"].mean(), 1)
            
            best_zip = latest.loc[latest["Avg_AQI"].idxmin()]
            worst_zip = latest.loc[latest["Avg_AQI"].idxmax()]

            col1, col2, col3 = st.columns(3)
            col1.metric("🌡 Avg AQI", avg_aqi, help="Overall average AQI for selected ZIP codes and period.")
            col2.metric("✅ Best ZIP", f"{best_zip['Zip_Code']} ({round(best_zip['Avg_AQI'],1)})",
                        help="ZIP code with lowest AQI in latest readings.")
            col3.metric("🔥 Worst ZIP", f"{worst_zip['Zip_Code']} ({round(worst_zip['Avg_AQI'],1)})",
                        help="ZIP code with highest AQI in latest readings.")

    # -------- AQI Category Distribution --------
    with subtab2:
        if filtered_df.empty:
            st.warning("No data available for selected filters.")
        else:
            def categorize_aqi(aqi):
                if aqi <= 50: return "Good"
                elif aqi <= 100: return "Moderate"
                elif aqi <= 150: return "Unhealthy (Sensitive)"
                elif aqi <= 200: return "Unhealthy"
                elif aqi <= 300: return "Very Unhealthy"
                else: return "Hazardous"

            filtered_df["AQI_Category"] = filtered_df["Avg_AQI"].apply(categorize_aqi)
            cat_counts = filtered_df["AQI_Category"].value_counts().reset_index()
            cat_counts.columns = ["Category", "Count"]

            category_order = [
                "Good", "Moderate", "Unhealthy (Sensitive)", 
                "Unhealthy", "Very Unhealthy", "Hazardous"
            ]

            cat_counts["Category"] = pd.Categorical(
                cat_counts["Category"], 
                categories=category_order, 
                ordered=True
            )
            cat_counts = cat_counts.sort_values("Category")

            color_map = {
                "Good": "#00e400",
                "Moderate": "#ffff00",
                "Unhealthy (Sensitive)": "#ff7e00",
                "Unhealthy": "#ff0000",
                "Very Unhealthy": "#8f3f97",
                "Hazardous": "#7e0023"
            }

            fig = px.pie(
                cat_counts,
                names="Category",
                values="Count",
                color="Category",
                color_discrete_map=color_map,
                title="AQI Category Distribution"
            )
            fig.update_traces(sort=False)  # <-- forces correct slice + legend order

            st.plotly_chart(fig, use_container_width=True)

# ---------------
# Trends Tab
# ---------------
with tab2:
    st.header("Time Trends")

    st.subheader("AQI Over Time")
    fig = px.line(filtered_df, x="Hour_Timestamp", y="Avg_AQI", color="Zip_Code",
                  title="Hourly AQI Trends")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Time-of-Day Heatmap")

    filtered_df["Hour"] = filtered_df["Hour_Timestamp"].dt.hour
    heatmap_data = filtered_df.groupby(["Hour", "Zip_Code"]).Avg_AQI.mean().reset_index()

    fig_heatmap = px.density_heatmap(
        heatmap_data, x="Hour", y="Zip_Code", z="Avg_AQI",
        color_continuous_scale="RdYlGn_r", title="Hourly AQI Patterns by ZIP"
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

# ---------------
# Map Tab
# ---------------
with tab3:
    st.header("Sensor Locations")

    latest_locations = filtered_df.sort_values("Hour_Timestamp").groupby("Sensor_ID").tail(1)

    fig_map = px.scatter_mapbox(
        latest_locations, lat="Latitude", lon="Longitude", color="Avg_AQI",
        size="Avg_PM2_5", hover_name="Zip_Code",
        color_continuous_scale="RdYlGn_r", zoom=10, height=500
    )
    fig_map.update_layout(mapbox_style="open-street-map")
    st.plotly_chart(fig_map, use_container_width=True)

# ---------------
# About Tab
# ---------------
with tab4:
    st.header("About This Dashboard")
    st.markdown("""
    This interactive dashboard displays simulated air quality data for Fresno, CA using PM2.5 and AQI metrics.
    
    **AQI Categories:**
    - Good (0–50)
    - Moderate (51–100)
    - Unhealthy for Sensitive Groups (101–150)
    - Unhealthy (151–200)
    - Very Unhealthy (201–300)
    - Hazardous (301+)
    
    The data is fully synthetic but modeled to reflect realistic seasonal air quality patterns in Fresno County.

    Created by [Your Name].
    """)
