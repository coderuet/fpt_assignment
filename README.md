# Weather-Sales Analysis Platform

This project implements a data pipeline and analytics platform to analyze the correlation between weather conditions and retail sales performance. It uses Apache Airflow for orchestration, PostgreSQL for data storage, and Grafana for visualization.

## Overview

The platform processes retail transaction data alongside weather information to provide insights into:

1. How weather affects retail revenue by region and store type
2. Which products sell better during specific weather conditions
3. How different store sizes respond to weather changes

## Architecture

- **Apache Airflow**: Orchestrates the entire data pipeline
- **PostgreSQL**: Stores both raw data and analytics results
- **Grafana**: Provides interactive dashboards for data visualization

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Make (optional, for convenience commands)

### Environment Configuration

1. Copy the example environment file:

   ```
   cp .env.example .env
   ```

2. Modify the `.env` file with your preferred settings:

   ```
   # Database settings
   MAIN_DB_USERNAME=main_db
   MAIN_DB_PASSWORD=password
   MAIN_DB_DATABASE_NAME=admin
   MAIN_DB_PORT=5432

   # Airflow settings
   AIRFLOW_SUPER_ADMIN_USERNAME=airflow
   AIRFLOW_SUPER_ADMIN_PASSWORD=airflow
   AIRFLOW_WEB_SERVER_PORTS=8080
   AIRFLOW_PROJ_DIR=.  # Base directory for Airflow files (dags, logs, etc.)

   # Grafana settings
   GRAFANA_ADMIN_USER=admin
   GRAFANA_ADMIN_PASSWORD=admin
   ```

### Preparing Data and Code

1. Create the necessary directories:

   ```
   mkdir -p data dags logs plugins config
   ```

2. Copy the DAG files to the dags directory:

   ```
   cp -r code/dags/* ./dags/
   ```

3. Ensure you have sample data files in the data directory:
   ```
   # Example data files needed:
   # - data/customers.csv
   # - data/discounts.csv
   # - data/employees.csv
   # - data/products.csv
   # - data/stores.csv
   # - data/transactions.csv
   ```

### Starting the Platform

1. Build the custom Airflow image:

   ```
   make build-image
   ```

2. Start all services:
   ```
   make start
   ```

### Stopping the Platform

```
make stop
```

To completely clean up all resources:

```
make clean
```

## Data Pipeline

The data pipeline consists of the following steps:

1. **Data Ingestion**: Load retail transaction data and product information
2. **Weather Data Enrichment**: Fetch and integrate weather data for store locations
3. **Data Transformation**: Create analytics data marts with weather-sales correlations
4. **Visualization**: Present insights through Grafana dashboards

## Dashboards

The platform includes pre-configured Grafana dashboards:

1. **Weather Sales Analysis Dashboard**: Shows:
   - Average revenue by weather condition (rainy vs. not rainy)
   - Top-selling products by weather condition
   - Revenue by store size and temperature range
   - Transaction count distribution by weather condition

## Project Structure

```
├── code/
│   ├── dags/           # Airflow DAG definitions
│   │   ├── workflow.py # Main data pipeline
│   │   ├── utils/      # Utility functions
│   │   └── sql/        # SQL queries
├── data/               # Data storage (gitignored)
├── grafana/
│   ├── dashboards/     # Dashboard definitions
│   └── provisioning/   # Grafana provisioning configs
├── docker-compose.yaml # Service definitions
├── Dockerfile          # Custom Airflow image
├── init.sql            # Database initialization
├── Makefile            # Convenience commands
└── README.md           # This file
```

## Extending the Platform

### Adding New Data Sources

1. Create a new ingestion task in the Airflow DAG
2. Add appropriate dimension and fact tables in `init.sql`
3. Update transformation logic in the DAG

### Creating New Visualizations

1. Design new queries based on the data marts
2. Add new panels to existing dashboards or create new ones
3. Update the dashboard JSON files in `grafana/dashboards/`

## Troubleshooting

- **Database Connection Issues**: Check the database credentials in `.env`
- **Missing Data**: Ensure the data ingestion tasks completed successfully
