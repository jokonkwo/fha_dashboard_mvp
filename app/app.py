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
    # Dynamic date range
    start_display = filtered_df["Hour_Timestamp"].min().strftime("%b %Y")
    end_display = filtered_df["Hour_Timestamp"].max().strftime("%b %Y")
    
    # Clean header + sub-header
    st.header("Air Quality Summary")
    st.markdown(
        f"<h5 style='color: grey; margin-top: -10px;'>({start_display} - {end_display})</h5>",
        unsafe_allow_html=True
    )

    # Inline ZIP filter inside Overview tab
    with st.expander("Filter ZIP Codes", expanded=False):
        selected_zips_inline = st.multiselect(
            "", zip_codes, default=selected_zips, label_visibility="collapsed"
        )
    filtered_df = filtered_df[filtered_df["Zip_Code"].isin(selected_zips_inline)]

    # Sub-tabs inside Overview tab
    subtab1, subtab2, subtab3 = st.tabs(["🔢 Summary Metrics", "🎯 AQI Categories", "📅 Custom Period Table"])

    # ---------------- Summary Metrics Sub-Tab ----------------
    with subtab1:
        latest = filtered_df.sort_values("Hour_Timestamp").groupby("Zip_Code").tail(1)
        avg_aqi = round(filtered_df["Avg_AQI"].mean(), 1)
        
        best_zip = latest.loc[latest["Avg_AQI"].idxmin()]
        worst_zip = latest.loc[latest["Avg_AQI"].idxmax()]

        col1, col2, col3 = st.columns(3)
        col1.metric("🌡 Avg AQI", avg_aqi, help="Overall average AQI for selected ZIP codes and dates.")
        col2.metric("✅ Best ZIP", f"{best_zip['Zip_Code']} ({round(best_zip['Avg_AQI'],1)})",
                    help="ZIP code with lowest AQI in latest readings.")
        col3.metric("🔥 Worst ZIP", f"{worst_zip['Zip_Code']} ({round(worst_zip['Avg_AQI'],1)})",
                    help="ZIP code with highest AQI in latest readings.")

    # ---------------- AQI Category Distribution Sub-Tab ----------------
    with subtab2:
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

        # Force correct category order
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
        st.plotly_chart(fig, use_container_width=True)

    # ---------------- Custom Date Range Table Sub-Tab ----------------
    with subtab3:
        df_table = filtered_df.copy()

        # Generate full month list
        min_month = df_table["Hour_Timestamp"].min().replace(day=1)
        max_month = df_table["Hour_Timestamp"].max().replace(day=1)
        date_options = pd.date_range(min_month, max_month, freq="MS").strftime("%b %Y").tolist()

        col_start, col_end = st.columns(2)
        start_month = col_start.selectbox("Start Month", date_options, index=0)
        end_month = col_end.selectbox("End Month", date_options, index=len(date_options)-1)

        # Convert selections back to datetime for filtering
        start_dt = pd.to_datetime(start_month, format="%b %Y")
        end_dt = pd.to_datetime(end_month, format="%b %Y") + pd.offsets.MonthEnd(1)

        # Filter table based on custom period
        period_df = df_table[
            (df_table["Hour_Timestamp"] >= start_dt) &
            (df_table["Hour_Timestamp"] <= end_dt)
        ]

        zip_summary = period_df.groupby("Zip_Code").agg(
            Avg_AQI=("Avg_AQI", "mean"),
            Max_AQI=("Avg_AQI", "max")
        ).reset_index().round(1)

        st.dataframe(zip_summary, use_container_width=True)

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
