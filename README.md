# mahesh_data_processing

There are multile ways to impliment it..

Approach 1: 
step 1 : Load the weather csv files into AWS S3.
step 2 : Create the DataBase, Crawler and table for the csv files which are loaded as part of the step 1.
step 3 : Create the Glue job to read the cvs data by using the table created for the csv data.
Step 4 : Write the dynamic frame to convert csv data parquet and write it to AWS S3.
Step 5 : Create the athena table for the parquet data which is loaded as part of the step 4.
Step 6 : Create the filter queries to get the desired result.

Approach 2: 

step 1 : Load the weather csv files into AWS S3.
step 2 : Create the DataBase, Crawler and table for the csv files which are loaded as part of the step 1.
step 3 : Use athena to convert csv to parquet data by creating the athena tables using csv table.
Step 4 : Create the filter queries to get the desired result.
