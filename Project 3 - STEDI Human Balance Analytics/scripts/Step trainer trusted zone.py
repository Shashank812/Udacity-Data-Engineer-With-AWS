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

# Script generated for node step_trainer_landing
step_trainer_landing_node1709451021890 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1709451021890",
)

# Script generated for node customer_curated
customer_curated_node1709451023961 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1709451023961",
)

# Script generated for node SQL Query
SqlQuery511 = """
select * from myDataSource
join myDataSource1
on myDataSource.serialnumber = myDataSource1.serialnumber
"""
SQLQuery_node1709452911025 = sparkSqlQuery(
    glueContext,
    query=SqlQuery511,
    mapping={
        "myDataSource": customer_curated_node1709451023961,
        "myDataSource1": step_trainer_landing_node1709451021890,
    },
    transformation_ctx="SQLQuery_node1709452911025",
)

# Script generated for node Drop Fields
DropFields_node1709451363432 = DropFields.apply(
    frame=SQLQuery_node1709452911025,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
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
    path="s3://shas-lake-house/step_trainer/trusted/",
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
