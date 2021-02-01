import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


client = boto3.client('glue', region_name='')

databaseName = 'weather-db'
print '\ndatabaseName: ' + databaseName

DB_Tables = client.get_tables(DatabaseName=databaseName)

tableList = DB_Tables['TableList']

for table in tableList:
    tableName = table['Name']

    we1 = glueContext.create_dynamic_frame.from_catalog(
        database="db_csv", 
        table_name=tableName, 
        transformation_ctx="we1"
    )

    df_write_parquet = glueContext.write_dynamic_frame.from_options(
        frame=we1,
        connection_type="s3", 
        connection_options={
            "path": "s3://mahesh_bucket/weather/"+ tableName + "/"
            },
        format="parquet",
        transformation_ctx="df_write_parquet"
    )
job.commit()
