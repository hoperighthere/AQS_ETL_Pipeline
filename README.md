# AQS_ETL_Pipeline
## Overview
This project implements an ETL (Extract, Transform, Load) pipeline for air quality data. It extracts daily air quality data for different states from the EPA's Air Quality System (AQS) API, transforms the data into a suitable format, and loads it into a PostgreSQL database for further analysis and visualization.
 
# Key Features
+ **Efficient Data Extraction with Dlt**: Dlt's capabilities are harnessed to efficiently extract air quality data from the API endpoint, reducing extraction time and resources.
+ **Python Scripting for ETL**: The ETL pipeline is entirely scripted in Python, using libraries like Pandas for data manipulation and transformation tasks.
+ **Secure Environment Variables Management**: API keys and sensitive information are securely managed and loaded using the dotenv library, ensuring data security.
+ **Robust Logging and Error Handling**: A comprehensive logging system is implemented to track the ETL process and handle potential errors effectively.
+ **Seamless Integration with PostgreSQL**: Transformed data seamlessly integrates with a PostgreSQL database, providing a structured and scalable storage solution.


