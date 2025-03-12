import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Snowflake Data Viewer", layout="wide")

# Initialize Snowflake connection
conn = st.connection("snowflake")
session = conn.session()

st.title("📊 Snowflake Data Viewer")

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
            st.subheader("📈 Visualizations and Insights")
            
            if table == "DAILY_INDEX_METRICS":
                fig = px.line(st.session_state.df, x="DATE", y="INDEX_VALUE", color="INDEX_NAME", title="Daily Index Values")
                st.plotly_chart(fig, use_container_width=True)
                st.info("This chart shows the daily fluctuations in index values. Sharp changes may indicate significant market events.")
                
                fig = px.bar(st.session_state.df, x="DATE", y="DAILY_RETURN", color="INDEX_NAME", title="Daily Returns")
                st.plotly_chart(fig, use_container_width=True)
                st.info("Daily returns represent the day-to-day percentage change in index values. Large positive or negative returns often correlate with major economic news or events.")
            
            elif table == "WEEKLY_INDEX_METRICS":
                fig = px.line(st.session_state.df, x="WEEK", y="AVG_INDEX_VALUE", color="INDEX_NAME", title="Weekly Average Index Values")
                st.plotly_chart(fig, use_container_width=True)
                st.info("Weekly averages smooth out daily fluctuations, providing a clearer trend of index performance over time.")
                
                fig = px.bar(st.session_state.df, x="WEEK", y="AVG_DAILY_RETURN", color="INDEX_NAME", title="Weekly Average Daily Returns")
                st.plotly_chart(fig, use_container_width=True)
                st.info("This chart shows the average daily return for each week. Consistent positive or negative weekly returns can indicate broader market trends.")
            
            elif table == "MONTHLY_INDEX_METRICS":
                fig = px.line(st.session_state.df, x="MONTH", y="AVG_INDEX_VALUE", color="INDEX_NAME", title="Monthly Average Index Values")
                st.plotly_chart(fig, use_container_width=True)
                st.info("Monthly averages provide a high-level view of index performance, useful for identifying long-term trends and seasonal patterns.")
                
                fig = px.bar(st.session_state.df, x="MONTH", y="MONTHLY_RETURN", color="INDEX_NAME", title="Monthly Returns")
                st.plotly_chart(fig, use_container_width=True)
                st.info("Monthly returns show the overall performance of indices over each month. This can be particularly useful for comparing index performance to broader economic indicators or events.")

st.sidebar.info("This app displays data from different schemas in Snowflake and provides visualizations with insights for analytics data.")
