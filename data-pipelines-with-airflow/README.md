# Data pipelines with Airflow

This folder contains an Apache Airflow pipeline as a solution to the sixth project in Udacity's Data Engineer Nanodegree program. It implements the DAG and several operators to perform an ETL from Amazon S3 to Amazon Redshift.

# Usage
You will need a working installation of Apache Airflow and the files in this folder into it. In addition, two connections will need to be set up in the Airflow UI:

| Conn Id           | Conn Type           | Host           | Schema            | Login          | Password             | Port        |
|-------------------|---------------------|----------------|-------------------|----------------|----------------------|-------------|
| aws_credentials   | Amazon Web Services |                |                   |<AWS Access Key>| <AWS SECRET KEY>     |             |
| redshift          | Postgres            | < Master URL>  | <e.g. dev>        | <e.g. awsuser> | <password>           | <e.g. 5439> |                   