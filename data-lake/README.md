# Data warehouse
This directory contains a solution to the fourth project of Udacity's *Data Engineer* Nanodegree program. It makes use of PySpark to perform a data transfomation from and to Amazon S3. The raw data list music songs and user interaction events from Sparkify, a fictitious music streamimg app. The data set being used (not contained in this folder) is a subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). It is provided in S3 Buckets on AWS. The process loads the data into a Spark cluster, transforms it into dimension and fact tables and writes the transformed data back into separate S3 buckets.

## Usage

### Requirements
To run this installation locally, you'll need Python 3 and a working installation of PySpark. The implementation also requires the *psycopg2* module available for import. You will need your own S3 bucket to write the transformed data to (and specify the corresponding S3 path in *output_data* in etl.py) and configure your AWS credentials in dl.cfg.
### Execution
To run the complete transformation pipeline just execute
```
python etl.py
```

## Why is the data moved to a database?
Since the original data about songs and user behaviour is only available in JSON files and potentially has a large volume it can not be easily analyzed regarding those questions. This application is an example of a data lake, where the original data is available for processing, and the transformed data is being put back into the lake for easier access by other data consumers.

