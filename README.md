# AQS_ETL_Pipeline
## Overview
This project implements an ETL (Extract, Transform, Load) pipeline specifically designed to automate the extraction, transformation, and loading of ozone data from the Air Quality Index (AQI) website's API into a PostgreSQL database. The pipeline focuses on extracting daily ozone measurements for different states and storing them for further analysis and visualization.
.
 
# Key Features
+ **Efficient Data Extraction with Dlt**: Dlt's capabilities are harnessed to efficiently extract air quality data from the API endpoint, reducing extraction time and resources.
+ **Python Scripting for ETL**: The ETL pipeline is entirely scripted in Python, using libraries like Pandas for data manipulation and transformation tasks.
+ **Secure Environment Variables Management**: API keys and sensitive information are securely managed and loaded using the dotenv library, ensuring data security.
+ **Robust Logging and Error Handling**: A comprehensive logging system is implemented to track the ETL process and handle potential errors effectively.
+ **Seamless Integration with PostgreSQL**: Transformed data seamlessly integrates with a PostgreSQL database, providing a structured and scalable storage solution.

## Data Source

The data used in this project is sourced from the Air Quality Index (AQI) website, provided by the Environmental Protection Agency (EPA) of the United States. [Air Quality System (AQS) API](https://aqs.epa.gov/aqsweb/documents/data_api.html)



### About the Air Quality Index (AQI) Website

The AQI website collects air quality data from monitoring stations located throughout the United States. It provides real-time and historical data on air pollutants such as ozone, particulate matter (PM2.5 and PM10), carbon monoxide (CO), sulfur dioxide (SO2), nitrogen dioxide (NO2), and more.


