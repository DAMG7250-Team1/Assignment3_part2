import subprocess
import sys

# Print installed packages to Streamlit logs
subprocess.run([sys.executable, "-m", "pip", "list"])

import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(page_title="Snowflake Data Viewer", layout="wide")

# Initialize Snowflake connection
conn = st.connection("snowflake")
session = conn.session()

st.title("\ud83d\udcca Snowflake Data Viewer")

# Sidebar for schema selection
schema = st.sidebar.selectbox("Select Schema", ["RAW_DOW30", "HARMONIZED_DOW30", "ANALYTICS_DOW30"])

# Function to load data from Snowflake
@st.cache_data
def load_data(schema, table):
    query = f"SELECT * FROM FRED_INDEX_DATA.{schema}.{table} LIMIT 1000"
    return session.sql(query).to_pandas()

# Main content area
col1, col2 = st.columns([1, 3])

with col1:
    st.subheader("Data Selection")
    if schema == "ANALYTICS_DOW30":
        table = st.selectbox("Select Analytics Table", ["DAILY_INDEX_METRICS", "WEEKLY_INDEX_METRICS", "MONTHLY_INDEX_METRICS"])
    else:
        table = "RAW_DOW30_STAGING" if schema == "RAW_DOW30" else "DOW30_HARMONIZED"
    
    if st.button("Load Data", key="load_data"):
        with st.spinner("Loading data..."):
            df = load_data(schema, table)
        st.session_state.df = df
        st.session_state.show_data = True

with col2:
    if 'show_data' in st.session_state and st.session_state.show_data:
        st.subheader(f"{schema} Data")
        st.dataframe(st.session_state.df)

        if schema == "ANALYTICS_DOW30":
            st.subheader("\ud83d\udcc8 Visualizations and Insights")
            
            if table == "DAILY_INDEX_METRICS":
                fig = alt.Chart(st.session_state.df).mark_line().encode(
                    x="DATE:T",
                    y="INDEX_VALUE:Q",
                    color="INDEX_NAME:N"
                ).properties(title="Daily Index Values")
                st.altair_chart(fig, use_container_width=True)
                
                fig = alt.Chart(st.session_state.df).mark_bar().encode(
                    x="DATE:T",
                    y="DAILY_RETURN:Q",
                    color="INDEX_NAME:N"
                ).properties(title="Daily Returns")
                st.altair_chart(fig, use_container_width=True)
            
            elif table == "WEEKLY_INDEX_METRICS":
                fig = alt.Chart(st.session_state.df).mark_line().encode(
                    x="WEEK:T",
                    y="AVG_INDEX_VALUE:Q",
                    color="INDEX_NAME:N"
                ).properties(title="Weekly Average Index Values")
                st.altair_chart(fig, use_container_width=True)
                
                fig = alt.Chart(st.session_state.df).mark_bar().encode(
                    x="WEEK:T",
                    y="AVG_DAILY_RETURN:Q",
                    color="INDEX_NAME:N"
                ).properties(title="Weekly Average Daily Returns")
                st.altair_chart(fig, use_container_width=True)
            
            elif table == "MONTHLY_INDEX_METRICS":
                fig = alt.Chart(st.session_state.df).mark_line().encode(
                    x="MONTH:T",
                    y="AVG_INDEX_VALUE:Q",
                    color="INDEX_NAME:N"
                ).properties(title="Monthly Average Index Values")
                st.altair_chart(fig, use_container_width=True)
                
                fig = alt.Chart(st.session_state.df).mark_bar().encode(
                    x="MONTH:T",
                    y="MONTHLY_RETURN:Q",
                    color="INDEX_NAME:N"
                ).properties(title="Monthly Returns")
                st.altair_chart(fig, use_container_width=True)

st.sidebar.info("This app displays data from different schemas in Snowflake and provides visualizations with insights for analytics data.")
