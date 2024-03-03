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

# Script generated for node customer landing
customerlanding_node1709103079763 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://shas-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customerlanding_node1709103079763",
)

# Script generated for node SQL Query- Privacy Filter
SqlQuery412 = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate != 0;
"""
SQLQueryPrivacyFilter_node1709380574451 = sparkSqlQuery(
    glueContext,
    query=SqlQuery412,
    mapping={"customer_landing": customerlanding_node1709103079763},
    transformation_ctx="SQLQueryPrivacyFilter_node1709380574451",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1709103419532 = glueContext.getSink(
    path="s3://shas-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node1709103419532",
)
TrustedCustomerZone_node1709103419532.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node1709103419532.setFormat("json")
TrustedCustomerZone_node1709103419532.writeFrame(
    SQLQueryPrivacyFilter_node1709380574451
)
job.commit()
