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

# Script generated for node customer_trusted
customer_trusted_node1709387440384 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1709387440384",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1709387421587 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1709387421587",
)

# Script generated for node Privacy Filter
SqlQuery496 = """
select * from myDataSource
join myDataSource1
on myDataSource.email = myDataSource1.user
"""
PrivacyFilter_node1709462971935 = sparkSqlQuery(
    glueContext,
    query=SqlQuery496,
    mapping={
        "myDataSource": customer_trusted_node1709387440384,
        "myDataSource1": accelerometer_landing_node1709387421587,
    },
    transformation_ctx="PrivacyFilter_node1709462971935",
)

# Script generated for node Drop Fields
DropFields_node1709387677878 = DropFields.apply(
    frame=PrivacyFilter_node1709462971935,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1709387677878",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1709464078473 = DynamicFrame.fromDF(
    DropFields_node1709387677878.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1709464078473",
)

# Script generated for node Amazon S3
AmazonS3_node1709387694989 = glueContext.getSink(
    path="s3://shas-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1709387694989",
)
AmazonS3_node1709387694989.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="customer_curated"
)
AmazonS3_node1709387694989.setFormat("json")
AmazonS3_node1709387694989.writeFrame(DropDuplicates_node1709464078473)
job.commit()
