import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1709451023961 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1709451023961",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1709451021890 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1709451021890",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
join myDataSourceone
on myDataSource.sensorreadingtime = myDataSourceone.timestamp
"""
SQLQuery_node1709452911025 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "myDataSource": step_trainer_trusted_node1709451023961,
        "myDataSourceone": accelerometer_trusted_node1709451021890,
    },
    transformation_ctx="SQLQuery_node1709452911025",
)

# Script generated for node Drop Fields
DropFields_node1709451363432 = DropFields.apply(
    frame=SQLQuery_node1709452911025,
    paths=[],
    transformation_ctx="DropFields_node1709451363432",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1709464694068 = DynamicFrame.fromDF(
    DropFields_node1709451363432.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1709464694068",
)

# Script generated for node Amazon S3
AmazonS3_node1709451367409 = glueContext.getSink(
    path="s3://shas-lake-house/step_trainer/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1709451367409",
)
AmazonS3_node1709451367409.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1709451367409.setFormat("json")
AmazonS3_node1709451367409.writeFrame(DropDuplicates_node1709464694068)
job.commit()
