# Philippines Earthquake Records ETL Pipeline
Automated data pipeline that extracts, transforms, and loads earthquake data from the Philippines into a PostgreSQL database using Airflow and dbt, with final dashboards displayed in Superset.

## Video Link
- **Youtube**: [Watch on YT](https://youtu.be/VQQe5uidCOk)
  
## Project Overview
The project automates the collection and processing of earthquake data from [PHIVOLCS](https://earthquake.phivolcs.dost.gov.ph/). It fetches raw earthquake data using **SOAP** for HTML parsing, removed duplicates and null using pandas, processes it with Python scripts, 
runs transformations with dbt, and loads it into PostgreSQL for analytics. The entire workflow is scheduled and managed using Apache Airflow, containerized with Docker, and visualized using Superset.

### Dashboard Preview
Here is an example Superset dashboard showing the processed weather metrics:

![Dashboard Example](images/superset1.PNG)

---

## Key Features
- Scheduled ETL pipeline using Airflow (PythonOperator & DockerOperator)
- Data transformation and modeling with dbt
- Automated aggregation of earthquake metrics:
  - Most active locations
  - Daily earthquakes count
  - Daily average magnitude
- Containerized using Docker and Docker Compose
- Visualized with Apache Superset

### My Dashboard Charts
![Dashboard](images/superset1.PNG)
![Dashboard](images/superset2.PNG)
![Dashboard](images/superset3.PNG)


---

## Technologies & Skills
- Python, SQL (PostgreSQL), HTML Parsing(Soap)
- Apache Airflow (DAGs, Operators, scheduling)
- Docker & Docker Compose
- dbt (data modeling & transformations)
- Apache Superset (visualization)

---
