# Automated Environmental Air Quality Data Pipeline

This project is a complete, automated data engineering pipeline designed to collect, store, and process daily environmental data for selected locations in Georgia, USA. The system automatically fetches data from weather and air quality APIs, processes it, and stores it in a PostgreSQL database, creating a rich dataset for analysis and machine learning.

## Key Features

* **Automated Daily ETL:** The pipeline runs automatically every day via Windows Task Scheduler, ensuring a consistent and growing dataset without manual intervention.
* **Multi-Source API Integration:** Extracts and combines data from two different external sources: the Google Air Quality API and the Open-Meteo API.
* **Robust Data Transformation:** Python scripts handle data cleaning, transformation, and aggregation of hourly data points into daily summaries.
* **Idempotent Database Loading:** The data loading process is designed to be idempotent using `ON CONFLICT DO UPDATE`, preventing duplicate records and ensuring data integrity if the script is run more than once on the same day.
* **Historical Data Backfill:** Includes a dedicated script to perform a one-time backfill of the last 30 days of historical data, providing an immediate dataset for analysis.

## Tech Stack

* **Language:** Python
* **Libraries:** Pandas, SQLAlchemy, Requests, python-dotenv
* **Database:** PostgreSQL
* **Automation:** Windows Task Scheduler, Batch Scripting
* **Version Control:** Git & GitHub

## System Architecture

The system operates on a single-machine architecture, where all processes run locally. The Python script, triggered by Task Scheduler, fetches data from the internet, transforms it, and loads it directly into the local PostgreSQL database.

```text
[ Internet APIs ]
      |
      | 1. Fetch Raw Data (JSON)
      V
[ Windows PC ]
 |            |
 |            | 2. Python ETL Script (Transforms Data)
 |            |
 V            V
[ PostgreSQL Database ] <---(3. Loads Data)
```
## Getting Started

Follow these steps to set up and run the project locally.

**1. Clone the Repository**

First, clone the project repository from GitHub to your local machine.

```bash
git clone [https://github.com/StanJohn04/AQI_Predict.git](https://github.com/StanJohn04/AQI_Predict.git)
cd AQI_Predict
```



**2. Set Up the Python Environment**

This project requires Anaconda (or Miniconda) with Python 3.12.

```bash
# Create and activate the Conda environment
conda create --name AQI_Predict python=3.12
conda activate AQI_Predict
```
```bash
# Install all required packages from the requirements file
pip install -r requirements.txt
```


**3. Configure the PostgreSQL Database**

Ensure you have a running PostgreSQL instance. You will need to create a dedicated user and database for this project. Once created, run the following script from your terminal to set up the necessary tables and schema:

```bash
psql -U your_user -d your_db_name -f scripts/database_setup.sql
```

**4. Create the Environment Variables File**

Create a file named `.env` in the root project directory. Populate this file with your specific API key and database credentials as outlined in the main project documentation.

**5. Run the ETL Scripts**

There are two main scripts to run:

* First, execute the one-time historical backfill to populate your database with the initial ~30 days of data:
    ```bash
    python scripts/historical_backfill.py
    ```
* The daily pipeline is designed to be run via the `run_etl.bat` script, which can then be automated with Windows Task Scheduler.

## Full Project Documentation

For a complete breakdown of the project architecture, implementation details, challenges faced, and future enhancement plans, please see the full [**Project Documentation PDF**](Docs/Docs.pdf).