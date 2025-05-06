import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1746521003586 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "multiLine": "false", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sales-data-pipeline-s3-bucket/raw_data/sales_data.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1746521003586")

# Script generated for node Change Schema
ChangeSchema_node1746521618406 = ApplyMapping.apply(frame=AmazonS3_node1746521003586, mappings=[("distributor name", "string", "distributor_name", "string"), ("dc number", "string", "dc_number", "int"), ("dc name", "string", "dc_name", "string"), ("sales month", "string", "sales_month", "int"), ("sales day", "string", "sales_day", "int"), ("sales year", "string", "sales_year", "int"), ("distributor sku", "string", "distributor_sku", "string"), ("shell sku number", "string", "shell_sku_number", "int"), ("distributor sku description", "string", "distributor_sku_description", "string"), ("dfoa quantity", "string", "dfoa_quantity", "int"), ("non-dfoa quantity", "string", "non_dfoa_quantity", "int"), ("unit of measure", "string", "unit_of_measure", "string")], transformation_ctx="ChangeSchema_node1746521618406")

# Script generated for node Remove Null Rows
RemoveNullRows_node1746521434752 = ChangeSchema_node1746521618406.gs_null_rows(extended=True)

# Script generated for node Distributors - Unique
SqlQuery7 = '''
select distinct(distributor_name) from sales_data
'''
DistributorsUnique_node1746522782479 = sparkSqlQuery(glueContext, query = SqlQuery7, mapping = {"sales_data":RemoveNullRows_node1746521434752}, transformation_ctx = "DistributorsUnique_node1746522782479")

# Script generated for node Products - Unique
SqlQuery8 = '''
select distinct(distributor_sku, shell_sku_number, distributor_sku_description) as distributor_sku, distributor_sku, 
shell_sku_number, distributor_sku_description from sales_data
'''
ProductsUnique_node1746523061951 = sparkSqlQuery(glueContext, query = SqlQuery8, mapping = {"sales_data":RemoveNullRows_node1746521434752}, transformation_ctx = "ProductsUnique_node1746523061951")

# Script generated for node Unit of Measures - Unique
SqlQuery9 = '''
select distinct unit_of_measure from sales_data
'''
UnitofMeasuresUnique_node1746536048230 = sparkSqlQuery(glueContext, query = SqlQuery9, mapping = {"sales_data":RemoveNullRows_node1746521434752}, transformation_ctx = "UnitofMeasuresUnique_node1746536048230")

# Script generated for node Warehouses - Unique
SqlQuery10 = '''
select distinct(dc_number), dc_name from sales_data
'''
WarehousesUnique_node1746522833097 = sparkSqlQuery(glueContext, query = SqlQuery10, mapping = {"sales_data":RemoveNullRows_node1746521434752}, transformation_ctx = "WarehousesUnique_node1746522833097")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746538742010 = glueContext.write_dynamic_frame.from_catalog(frame=RemoveNullRows_node1746521434752, database="sales-data-pipeline-etl-db", table_name="raw_sales_data", additional_options={"partitionKeys": ["distributor_name", "dc_number", "shell_sku_number", "unit_of_measure"]}, transformation_ctx="AWSGlueDataCatalog_node1746538742010")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746535751008 = glueContext.write_dynamic_frame.from_catalog(frame=DistributorsUnique_node1746522782479, database="sales-data-pipeline-etl-db", table_name="distributors", transformation_ctx="AWSGlueDataCatalog_node1746535751008")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746538048974 = glueContext.write_dynamic_frame.from_catalog(frame=ProductsUnique_node1746523061951, database="sales-data-pipeline-etl-db", table_name="products", transformation_ctx="AWSGlueDataCatalog_node1746538048974")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746538095357 = glueContext.write_dynamic_frame.from_catalog(frame=UnitofMeasuresUnique_node1746536048230, database="sales-data-pipeline-etl-db", table_name="unit_of_measures", transformation_ctx="AWSGlueDataCatalog_node1746538095357")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746538079865 = glueContext.write_dynamic_frame.from_catalog(frame=WarehousesUnique_node1746522833097, database="sales-data-pipeline-etl-db", table_name="warehouses", transformation_ctx="AWSGlueDataCatalog_node1746538079865")

job.commit()