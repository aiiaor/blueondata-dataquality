# Data Quality Project - README.md

## Overview
This project implements a data quality and cleaning pipeline for e-commerce data using Apache Airflow. The pipeline extracts data from a PostgreSQL database, performs various cleaning operations, validates the data quality, and loads the cleaned data back into the database.

## Features
- **Data Extraction:** Connects to PostgreSQL database to extract raw e-commerce order data.
- **Data Cleaning:**
  - Standardizes date formats.
  - Normalizes text fields (customer names, product names).
  - Handles missing values.
  - Removes duplicates.
  - Adds computed columns (e.g., `total_cost`).
- **Data Quality Checks:**
  - Validates non-null constraints.
  - Verifies data types.
  - Ensures values are within expected ranges.
- **Data Loading:** Saves cleaned data back to the database.

## Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Git

### Installation
Clone the repository:
```bash
git clone https://github.com/aiiaor/blueondata-dataquality.git
cd dataquality
```
For the first time, init Airflow first:
```bash
docker-compose up airflow-init
```

Start the Airflow services:
```bash
docker-compose up -d
```

## Usage
1. Access the Airflow UI at [http://localhost:8080](http://localhost:8080).
2. Trigger the `data_cleaning_ecommerce_postgres` DAG manually or wait for the scheduled run.

## Common Issues and Solutions

### PostgreSQL Connection Issues
If you encounter connection errors, ensure:
- PostgreSQL service is running.
- Connection parameters are correct.
- Network connectivity between Airflow and PostgreSQL is established.

### Date Format Issues
If you encounter date parsing errors, update the date format in the `standardize_formats` function to match your data format.

### NaT Values in Database
If you encounter `NaT` values, implement a solution to handle them before saving to the database.

## License
This project is licensed under the MIT License - see the LICENSE file for details.
