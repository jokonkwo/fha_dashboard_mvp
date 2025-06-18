import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ---------------
# Load Data
# ---------------
@st.cache_data
def load_data():
    conn = duckdb.connect("../data/dummy_air_quality.duckdb", read_only=True)
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
st.title("🌫 Fresno Air Quality Dashboard")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Trends", "🗺 Map", "ℹ️ About"])

# ---------------
# Overview Tab
# ---------------
with tab1:
    st.header("Air Quality Summary")

    latest = filtered_df.sort_values("Hour_Timestamp").groupby("Zip_Code").tail(1)
    avg_aqi = round(filtered_df["Avg_AQI"].mean(), 1)
    worst_zip = latest.loc[latest["Avg_AQI"].idxmax(), "Zip_Code"]
    worst_aqi = latest["Avg_AQI"].max()

    col1, col2, col3 = st.columns(3)
    col1.metric("🌡 Avg AQI", avg_aqi)
    col2.metric("📍 Worst ZIP", worst_zip)
    col3.metric("🔥 Max AQI", int(worst_aqi))

    # Pie chart of AQI Categories
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

    fig = px.pie(cat_counts, names="Category", values="Count", title="AQI Category Distribution")
    st.plotly_chart(fig, use_container_width=True)

    # Monthly ZIP Table
    st.subheader("Monthly AQI by ZIP")
    df_table = filtered_df.copy()
    df_table["Year"] = df_table["Hour_Timestamp"].dt.year
    df_table["Month"] = df_table["Hour_Timestamp"].dt.month_name()

    year_options = sorted(df_table["Year"].unique(), reverse=True)
    selected_year = st.selectbox("Year", year_options)
    month_options = df_table[df_table["Year"] == selected_year]["Month"].unique()
    selected_month = st.selectbox("Month", sorted(month_options))

    month_df = df_table[(df_table["Year"] == selected_year) & (df_table["Month"] == selected_month)]
    zip_summary = month_df.groupby("Zip_Code").agg(
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
