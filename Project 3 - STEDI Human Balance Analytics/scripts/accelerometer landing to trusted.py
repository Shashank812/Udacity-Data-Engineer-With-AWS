import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


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
customer_trusted_node1709387440384 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://shas-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1709387440384",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1709387421587 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://shas-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1709387421587",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
join myDataSource1
on myDataSource.email = myDataSource1.user
"""
SQLQuery_node1709453830706 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "myDataSource": customer_trusted_node1709387440384,
        "myDataSource1": accelerometer_landing_node1709387421587,
    },
    transformation_ctx="SQLQuery_node1709453830706",
)

# Script generated for node Drop Fields
DropFields_node1709387677878 = DropFields.apply(
    frame=SQLQuery_node1709453830706,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1709387677878",
)

# Script generated for node Amazon S3
AmazonS3_node1709387694989 = glueContext.getSink(
    path="s3://shas-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1709387694989",
)
AmazonS3_node1709387694989.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1709387694989.setFormat("json")
AmazonS3_node1709387694989.writeFrame(DropFields_node1709387677878)
job.commit()
