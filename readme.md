Codelabs :- https://codelabs-preview.appspot.com/?file_id=1yTTiaZAiFRhD4AUw89ZexKs6zS3WV6zxnX79wiVsE_Y/edit?tab=t.0#0

# FRED INDEX DATA PIPELINE

## PROJECT STRUCTURE
fred_index_pipeline/
|
├── data_ingestion/
│   ├── 01_fred_data_fetcher.py 
├── snowflake_setup/
│   ├── 02_snowflake_setup.sql
│   ├── 03_load_raw_raw_data_procedure.sql
│   └── 04_update_metrics_procedure.sql
├── data_processing/
│   ├── 04_harmonization_data_transformation.py
│ 
│   
├── analytics/
│   ├── 06_analytics_tables.py
│ 
├── orchestration/
│   ├── 06_pipeline_tasks.sql
│   ├── streamflow.py
│   └── deploy_demo_objects.yml
├── frontend/
│   ├── streamlitapp.py
│   
├── config/
│   ├── config.omn
│   └── environment.yml
├── .gitignore
├── README.md
└── requirements.txt

---

### Data Flow
1. **Data Ingestion** (S3/Snowflake)
   - Fetch from FRED API (01_fred_data_fetcher.py)
   - Store in S3 bucket
   - Load to Snowflake RAW_FRED schema

2. **Processing**
   - Raw → Harmonized (05_harmonization_data_transformation.py)
   - Standardize index formats
   - Calculate returns (daily/monthly)

3. **Analytics**
   - Create derived metrics (volatility, correlations)
   - Build aggregate tables
   - Generate reports

4. **Frontend** (Streamlit)
   - Interactive dashboards
   - Index comparison tools
   - Historical trend visualizations

Pipeline Execution
Follow the step-by-step notebooks in the directory:

Data Ingestion - 01.01_fred_data_fetcher.py

Load Raw Data - 02_snowflake_setup.sql Load the CSV data into Snowflake's raw layer
                03_load_raw_data_procedure.sql

Data Harmonization -04_harmozization_data_transformation.py
                     03_update_metrics_procedure.sql
Analytics Generation - 05_analytics_table.py Generate metrics and insights

Orchestration - run deploy_snowpark.py

5. Disclosures

WE ATTEST THAT WE HAVEN'T USED ANY OTHER STUDENTS' WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK

We acknowledge that all team members contributed equally and worked to present the final project provided in this submission. All participants played a role in crucial ways, and the results reflect our collective efforts.

Additionally we acknowledge we have leveraged use of AI along with the provided references for code updation, generating suggestions and debugging errors for the varied issues we faced through the development process.AI tools like we utilized:

ChatGPT
Perplexity
Cursor
Claude
Team Members

Contributions

Husain

33%

Sahil Kasliwal

33%

Dhrumil Patel

33%

