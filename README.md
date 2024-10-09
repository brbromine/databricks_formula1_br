# Databricks & PySpark Formula1 Project
This project was the part of Azure Databricks & Spark For Data Engineers (PySpark / SQL) by Ramesh Retnasamy

## Project overview
This project focuses on creating a data analysis pipeline for Formula 1 race data, utilizing Databricks and PySpark. The data is sourced from the Ergast API, a repository for comprehensive Formula 1 statistics. The pipeline involves extracting, transforming, and loading (ETL) the race data into a storage solution for further analysis and reporting. Azure Databricks is leveraged for data processing and transformation, while the data itself is stored in Azure Data Lake. The project aims to provide insightful analysis by efficiently handling and preparing the data.

## Formula 1 Overview 
Formula 1 (F1) is a premier motorsport competition, where drivers compete in high-speed races held on circuits across the globe. Each season consists of around 20-23 races, called Grands Prix. The competition features 10 teams, with each team fielding 2 drivers, totaling 20 drivers per race. Drivers earn points based on their finishing positions in each race, with points awarded to the top 10 finishers. The winner receives 25 points, second place gets 18, and the points decrease down to 1 point for the 10th position. These points contribute to both the Drivers' and Constructors' Championships, which crown the best individual driver and team at the end of the season.

## Solution Architecture
![Solution Architecture](https://github.com/user-attachments/assets/bae7f5bf-212f-4f2b-bbe8-903f3746bddf)

## ER Diagram
![image](https://github.com/user-attachments/assets/326aff60-1420-4584-a5e2-6d3a0398ae7a)

## Briefly Method

### ETL Pipeline
Azure Data Factory has 3 pipelines which are
  - Main Pipeline: Main pipeline will be executed following by trigger of every Sunday on 10 p.m. (after the race). This pipeline will execute 2 inside pipeline to run further
    ![image](https://github.com/user-attachments/assets/3916c4a8-e21c-438d-9c49-c79a23fba9b5)

    - Execute Ingestion: This pipeline will check if there is incoming data whether there exist or not based on dynamic file_date of ADF. If there is no race in that week, it would not do anything. In the other hand, the pipeline will run all the ingestion notebooks which include add, drop, rename column and save as managed delta lake table.
    - Execute Transformation: This pipeline will check the result after ingestion step, if there is processed data then it will execute necessary data to be shown in dashboard such as drivers standing, constructor standing and recalculated points (the points system in different years calculates different points)
   
## Analysis report

![image](https://github.com/user-attachments/assets/634b8a78-fee1-4618-bf69-e827b51865a8)

![image](https://github.com/user-attachments/assets/0e691a09-4c1f-40b4-88cd-c1d85feef99d)

## Task performed

- Created and used Azure Databricks service and the architecture of Databricks within Azure.

- Worked with Databricks notebooks and used Databricks utilities, magic commands, etc.

- Passed parameters between notebooks as well as created notebook workflows.

- Created, configured, and monitored Databricks clusters, cluster pools, and jobs.

- Mounted Azure Storage in Databricks using secrets stored in Azure Key Vault.

- Worked with Databricks Tables, Databricks File System (DBFS), etc.

- Used Delta Lake to implement a solution using Lakehouse architecture.

- Created dashboards to visualize the outputs.

## PySpark and Spark SQL

- Ingest simple and complex type of CSV and JSON file into notebook.
- Trasform such as Filter, Join, Simple Aggregations, GroupBy, Window functions etc.
- Create Database, Table, Global or Temporary View.
- Implement ingest full load and incremental load into notebook.

## Delta Lake

- Performed Read, Write, Update, Delete, and Merge to delta lake using both PySpark as well as SQL.
- History, Time Travel, and Vacuum.
- Converted Parquet files to Delta files.
- Implemented incremental load pattern using delta lake.

## Azure Data Factory

- Created pipelines to execute Databricks notebooks.
- Designed robust pipelines to deal with unexpected scenarios such as missing files.
- Created dependencies between activities as well as pipelines.
- Scheduled the pipelines using data factory triggers to execute at regular intervals.
- Monitored the triggers/ pipelines to check for errors/ outputs.

## About this project

### Folders
1. analysis: contain the analysis of data
2. demo: temporary path to try some function in this project
3. Formula1-Project-Solutions: guide from Ramesh Retnasamy in the course (Thank you for your dedication)
4. includes: contains notebooks with common functions and path configurations
5. ingestion: all notebook that needed to be ingest from every raw data
6. raw: create raw table from the location in Azure Date Lake Storage Gen2
7. set-up: demonstrate different ways to access Azure Data Lake Gen2 containers into the Databricks file system.
8. trans: transform all necessary data to be used to visualize in analysis step

### Tools
1. PySpark
2. Spark SQL
3. Delta lake
4. Azure Databricks
5. Azure Data Factory
6. Azure Data Lake Storage Gen 2
7. Azure Key Vault


