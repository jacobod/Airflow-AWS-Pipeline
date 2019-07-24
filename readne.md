# Airflow in AWS Project

### Building a S3 -> Redshift Data Pipeline using Apache Airflow

DAG that loads in S3 files (either as csv or json), stages them in Redshift tables,
then transforms them into analytics tables. Finally, a data quality check is run to make sure that the load matched expected results.

This project was created as part of Udacity's Data Engineering Nanodegree program.

### Setup

This project requires Apache Airflow, you can install the latest version with

    pip install apache-airflow

 Clone this repo into your desired location, then start your local airflow server.
 
