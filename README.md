# Data Engineering Project: Chicago Police Department

## Project Overview

This project is a comprehensive system for managing and analyzing offender data for the Chicago Police Department, specifically focusing on the Rogers Park and Lincoln Park areas. The project involves data collection, pre-processing, cleansing, and visualization using various technologies, including Flask, SQLAlchemy, Airflow, Google BigQuery, and Looker Studio.

## Problem Statement

The Chicago Police Department requires an efficient system to manage and analyze offender data from different precincts, specifically Rogers Park and Lincoln Park. The goal is to streamline the booking process, ensure data consistency, and derive actionable insights to improve law enforcement strategies and resource allocation.

## Project Structure

- **Flask Application**: Provides a web interface for booking offenders.
- **SQLAlchemy Models**: Defines the database schema and ORM models.
- **Airflow DAGs**: Automates data pre-processing, cleansing, and analysis tasks.
- **SQL Scripts**: Combines and cleanses data, and derives insights.
- **Looker Studio**: Visualizes the processed data.
- **Docker**: Containerizes the application and its dependencies.

## Data Collection and Storage

### Flask Application

- The Flask application provides a web interface for booking offenders in Rogers Park and Lincoln Park.
- Offender details are collected through forms (`booking_form.html` for Rogers Park and `lincoln_booking_form.html` for Lincoln Park).
- The collected data includes personal details (name, DOB, address, phone, SSN), demographic information (race, sex, ethnicity), and offenses.
- The data is stored in a SQL Server database using SQLAlchemy models defined in `app.py`.

### SQLAlchemy Models

- **Rogers Park**: Data is stored in tables within the `Rogers_Park` schema, including `Offender`, `Offense`, `Booking`, and `BookingOffense`.
- **Lincoln Park**: Data is stored in tables within the `Lincoln_Park` schema, including `lincoln_Offender`, `lincoln_Offense`, `lincoln_Booking`, and `lincoln_BookingOffense`.

## Data Pre-Processing and Cleansing

### Airflow DAGs

- The project uses Apache Airflow to automate the data pre-processing and cleansing tasks.
- The DAG defined in `dags/pre_processing.py` extracts SQL files from a specified folder, executes them, and loads the data into Google BigQuery.
- SQL scripts in the `dags/sql/pre_processing` directory combine and cleanse data from both Rogers Park and Lincoln Park, creating unified tables in BigQuery.

### SQL Scripts

- **Combined Offender**: `02_combined_offender.sql` combines offender data from both areas.
- **Combined Booking**: `01_combined_booking.sql` combines booking data.
- **Combined Booking Offense**: `03_combined_booking_offense.sql` combines booking offense data.
- **Combined Offense Codes**: `04_combined_offense_codes.sql` combines offense codes.

## Data Analysis and Insights

### Airflow DAGs

- Another DAG defined in `dags/extract_courts.py` extracts table metadata from SQL Server and loads it into Google BigQuery.
- The data is further processed to derive insights and metrics.

### SQL Scripts for Insights

- **All Offenses Base**: `01_all_offenses_base.sql` creates a base table for all offenses.
- **Metrics**: Various SQL scripts in the `dags/sql/public_dashboards` directory derive metrics such as the count of all offenders, top offenders, and demographic breakdowns.

## Dashboard Creation

### Looker Studio

- The cleansed and processed data in Google BigQuery is visualized using Looker Studio.
- The dashboard provides insights into offender data, including the number of offenses, top offenders, and demographic information.
- An embedded Looker Studio report is included in the `index.html` template, providing an interactive dashboard for users.

## Orchestration with Docker

### Docker and Docker Compose

- The project uses Docker to containerize the application and its dependencies, ensuring consistency across different environments.
- The `Dockerfile` sets up the Airflow environment with necessary dependencies.
- The `docker-compose.yaml` file orchestrates the services, including Airflow webserver, scheduler, and PostgreSQL database.
- Environment variables for Airflow configuration, including SMTP settings for email notifications, are defined in the `docker-compose.yaml` file.

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Python 3.8+
- Google Cloud Account (for BigQuery)

### Installation

1. **Clone the repository**:
   ```sh
   git clone https://github.com/Datartic/Data_Engineering_Project_Chicago_PD.git
   cd Data_Engineering_Project_Chicago_PD
   ```

2. **Build and start the Docker containers**:
   ```sh
   docker-compose up --build
   ```

3. **Initialize the database**:
   ```sh
   docker-compose run webserver airflow db init
   ```

4. **Access the Airflow web interface**:
   Open your browser and go to [http://localhost:8080](http://localhost:8080).

5. **Access the Flask application**:
   Open your browser and go to [http://localhost:5000](http://localhost:5000).

## Usage

- **Booking Offenders**: Use the Flask application to book offenders in Rogers Park and Lincoln Park.
- **Data Pre-Processing**: Airflow DAGs will automatically process and cleanse the data.
- **Data Analysis**: Use the SQL scripts to derive insights and metrics.
- **Dashboard**: View the interactive dashboard in Looker Studio for visual insights.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.