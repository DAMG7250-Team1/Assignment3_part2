import streamlit as st
import snowflake.connector
import pandas as pd
import altair as alt

# Streamlit app configuration
st.set_page_config(page_title="Snowflake Data Viewer", layout="wide")

# Connect to Snowflake using Streamlit secrets
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
    )

conn = get_snowflake_connection()
st.success("✅ Connected to Snowflake successfully!")

# Function to load data from Snowflake
@st.cache_data
def load_data(schema, table):
    query = f"SELECT * FROM {schema}.{table} LIMIT 1000"
    with conn.cursor() as cur:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df

# Streamlit UI
st.title("📊 Snowflake Data Viewer")

# Sidebar Schema Selection
schema = st.sidebar.selectbox("Select Schema", ["RAW_DOW30", "HARMONIZED_DOW30", "ANALYTICS_DOW30"])

# Choose Table Based on Schema
if schema == "ANALYTICS_DOW30":
    table = st.sidebar.selectbox("Select Analytics Table", ["DAILY_INDEX_METRICS", "WEEKLY_INDEX_METRICS", "MONTHLY_INDEX_METRICS"])
else:
    table = "RAW_DOW30_STAGING" if schema == "RAW_DOW30" else "DOW30_HARMONIZED"

# Load Data Button
if st.button("Load Data"):
    with st.spinner("Loading data..."):
        df = load_data(schema, table)
        st.session_state.df = df
        st.session_state.show_data = True

# Display Data and Visualizations
if "df" in st.session_state and st.session_state.show_data:
    st.subheader(f"📋 {schema} - {table} Data")
    st.dataframe(st.session_state.df)

    # Altair Visualization for Analytics Schema
    if schema == "ANALYTICS_DOW30":
        st.subheader("📈 Data Visualizations")

        if table == "DAILY_INDEX_METRICS":
            chart1 = alt.Chart(st.session_state.df).mark_line().encode(
                x="DATE:T", y="INDEX_VALUE:Q", color="INDEX_NAME:N"
            ).properties(title="Daily Index Values")
            st.altair_chart(chart1, use_container_width=True)

            chart2 = alt.Chart(st.session_state.df).mark_bar().encode(
                x="DATE:T", y="DAILY_RETURN:Q", color="INDEX_NAME:N"
            ).properties(title="Daily Returns")
            st.altair_chart(chart2, use_container_width=True)

        elif table == "WEEKLY_INDEX_METRICS":
            chart1 = alt.Chart(st.session_state.df).mark_line().encode(
                x="WEEK:T", y="AVG_INDEX_VALUE:Q", color="INDEX_NAME:N"
            ).properties(title="Weekly Average Index Values")
            st.altair_chart(chart1, use_container_width=True)

            chart2 = alt.Chart(st.session_state.df).mark_bar().encode(
                x="WEEK:T", y="AVG_DAILY_RETURN:Q", color="INDEX_NAME:N"
            ).properties(title="Weekly Average Daily Returns")
            st.altair_chart(chart2, use_container_width=True)

        elif table == "MONTHLY_INDEX_METRICS":
            chart1 = alt.Chart(st.session_state.df).mark_line().encode(
                x="MONTH:T", y="AVG_INDEX_VALUE:Q", color="INDEX_NAME:N"
            ).properties(title="Monthly Average Index Values")
            st.altair_chart(chart1, use_container_width=True)

            chart2 = alt.Chart(st.session_state.df).mark_bar().encode(
                x="MONTH:T", y="MONTHLY_RETURN:Q", color="INDEX_NAME:N"
            ).properties(title="Monthly Returns")
            st.altair_chart(chart2, use_container_width=True)

# Sidebar Info
st.sidebar.info("🔹 This app displays data from Snowflake schemas and provides visual analytics.")
