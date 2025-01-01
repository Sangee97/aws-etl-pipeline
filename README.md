# aws-etl-pipeline
This project will integrate AWS services into the ETL pipeline. Along with extracting, transforming, and loading data from CSV, JSON, and XML formats.Using AWS S3 for storage and retrieval of raw and transformed data and Using AWS RDS (Relational Database Service) to load transformed data.

Extracting process:

Created an s3 bucket in aws and uploaded the source files in bucket using BOTO3 in python
Downloaded all the files from s3 bucket and extracted all the files, convert them into dataframe
combining all the extracted files and make it as a single dataframe
Transforming process:
Performed some unit conversions (inches to metres, pounds to kilograms).
Clean and standardise the data.
Loading process:
After the data is transformed, store the resulting CSV file in the new S3 bucket with the name of transformed_data.Loaded the transformed data into relational database table.
Used sql.connector to upload the transformed file in my local sql server.
Used sqlalchemy to fetch the table uploaded in the AWS RDS.
Logging:
Use Python’s logging library to track the progress of the extraction, transformation, and loading phases.
Save the logs in a text file and optionally upload them to S3.
