Based on the repository files provided, here is a comprehensive `README.md` that explains the Airflow DAG automation and the data pipeline logic.

---

# Data Ingestion & Automation Pipeline

This repository contains an Apache Airflow-based automation system designed to simulate and manage daily data ingestion from flat files into a centralized data warehouse. It focuses on handling customer address updates, cleaning raw sales data, and maintaining downstream analytical datamarts.

## 🚀 Project Overview

The core of this project is a set of Airflow DAGs that automate the end-to-end data lifecycle:

1. **Detection**: Monitoring a landing zone for daily CSV files.
2. **Ingestion**: Fetching, cleaning, and standardizing raw data.
3. **Transformation**: Mapping regional data (cities/provinces) to a standardized format.
4. **Loading**: Appending processed records to a MySQL database.
5. **Datamart Refresh**: Triggering SQL reports once ingestion is successful.

## 🏗 Workflow Architecture

### 1. Auto-Ingest Pipeline (`auto_ingest_customer_address`)

This is the primary production DAG that runs on a `@daily` schedule.

* 
**File Sensor**: Uses a `FileSensor` to wait for a daily CSV file (e.g., `customer_address_20260315.csv`) to appear in the designated input folder.


* **Ingestion Logic**: Calls a Python-based pipeline that:
* Reads the CSV using **Polars** for high-performance data handling.


* Standardizes text (stripping whitespace, uppercasing, and removing newlines).


* 
**Region Mapping**: Uses a JSON-based mapping (`MappingDaerah.json`) to standardize inconsistent city and province names into a unified format.


* 
**MySQL Load**: Appends the cleaned data into the target table.


* 
**Archive**: Moves the processed file to an archive folder to prevent re-processing.




* 
**Downstream Updates**: Upon successful ingestion, it triggers two SQL tasks to update the `sales_monthly_report` and `yearly_report` datamarts.



### 2. Cleaning Pipeline (`clean_raw_tables`)

A utility DAG used for cleaning and recasting existing raw tables independently.

* 
**Sales Cleaning**: Converts price strings (e.g., `350.000.000`) into standardized integers and replaces the raw table with the cleaned version.


* 
**Customer Cleaning**: Parses ambiguous date-of-birth (DOB) formats (handling `YYYY-MM-DD`, `YYYY/MM/DD`, and `DD/MM/YYYY`) and handles "NULL" or empty strings.



## 📂 Repository Structure

```text
├── dags/
[cite_start]│   ├── auto_ingest_dag.py    # Main automation DAG with FileSensor [cite: 1]
[cite_start]│   ├── cleaning_dag.py       # DAG for manual/independent data cleaning [cite: 2]
[cite_start]│   ├── ingest_logic.py       # Core Polars logic for reading/mapping/loading [cite: 4]
[cite_start]│   ├── cleaning_logic.py     # Logic for data type recasting and date parsing [cite: 3]
│   └── datamart/             # SQL scripts for analytical reports
├── data/
[cite_start]│   ├── MappingDaerah.json    # Dictionary for city/province standardization [cite: 4]
│   └── schema_plan.tsv       # Technical metadata/mapping definitions
├── initFile/
│   ├── schema.sql            # Initial MySQL table definitions
│   └── migrate.py            # Script for initial data migration
└── docker-compose.yml        # Infrastructure setup (Airflow + MySQL)

```

## 🛠 Tech Stack

* 
**Orchestration**: Apache Airflow 


* 
**Data Processing**: Polars (used for schema enforcement and transformations) 


* 
**Database**: MySQL (acting as the Data Warehouse/Lake) 


* 
**Environment**: Docker Compose 



## 🔧 Setup & Usage

1. **Requirements**: Ensure you have the necessary Python libraries installed, including `polars`, `sqlalchemy`, and `pymysql`.
2. 
**Environment Variables**: The DAGs rely on Airflow Variables for folder paths and connection strings (e.g., `custaddr_folder`, `custaddr_mysql_conn`).


3. **File Landing**: To trigger the ingestion, place a CSV file named `customer_address_YYYYMMDD.csv` in the base folder. The `FileSensor` will detect it and start the process automatically.
