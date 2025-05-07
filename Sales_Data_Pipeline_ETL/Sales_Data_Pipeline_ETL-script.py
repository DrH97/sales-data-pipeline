import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsglue.dynamicframe import DynamicFrame
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

# Script generated for node RDS - Warehouses
RDSWarehouses_node1746559271387 = glueContext.create_dynamic_frame.from_catalog(database="sales-data-pipeline-etl-db", table_name="postgres_public_warehouses", transformation_ctx="RDSWarehouses_node1746559271387")

# Script generated for node Amazon S3
AmazonS3_node1746521003586 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "multiLine": "false", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sales-data-pipeline-s3-bucket/raw_data/sales_data.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1746521003586")

# Script generated for node RDS - Products
RDSProducts_node1746559845938 = glueContext.create_dynamic_frame.from_catalog(database="sales-data-pipeline-etl-db", table_name="postgres_public_products", transformation_ctx="RDSProducts_node1746559845938")

# Script generated for node RDS - Distributors
RDSDistributors_node1746558730068 = glueContext.create_dynamic_frame.from_catalog(database="sales-data-pipeline-etl-db", table_name="postgres_public_distributors", transformation_ctx="RDSDistributors_node1746558730068")

# Script generated for node RDS - Unit of Measures
RDSUnitofMeasures_node1746559628503 = glueContext.create_dynamic_frame.from_catalog(database="sales-data-pipeline-etl-db", table_name="postgres_public_units_of_measures", transformation_ctx="RDSUnitofMeasures_node1746559628503")

# Script generated for node Change Schema
ChangeSchema_node1746521618406 = ApplyMapping.apply(frame=AmazonS3_node1746521003586, mappings=[("distributor name", "string", "distributor_name", "string"), ("dc number", "string", "dc_number", "int"), ("dc name", "string", "dc_name", "string"), ("sales month", "string", "sales_month", "int"), ("sales day", "string", "sales_day", "int"), ("sales year", "string", "sales_year", "int"), ("distributor sku", "string", "distributor_sku", "string"), ("shell sku number", "string", "shell_sku_number", "int"), ("distributor sku description", "string", "description", "string"), ("dfoa quantity", "string", "dfoa_quantity", "int"), ("non-dfoa quantity", "string", "non_dfoa_quantity", "int"), ("unit of measure", "string", "unit_of_measure", "string")], transformation_ctx="ChangeSchema_node1746521618406")

# Script generated for node Remove Null Rows
RemoveNullRows_node1746521434752 = ChangeSchema_node1746521618406.gs_null_rows(extended=True)

# Script generated for node Products - Unique
SqlQuery45 = '''
select distinct(distributor_sku, shell_sku_number, description) as distributor_sku, distributor_sku, 
shell_sku_number, description from sales_data
'''
ProductsUnique_node1746523061951 = sparkSqlQuery(glueContext, query = SqlQuery45, mapping = {"sales_data":RemoveNullRows_node1746521434752}, transformation_ctx = "ProductsUnique_node1746523061951")

# Script generated for node Unit of Measures - Unique
SqlQuery46 = '''
select distinct unit_of_measure from sales_data
'''
UnitofMeasuresUnique_node1746536048230 = sparkSqlQuery(glueContext, query = SqlQuery46, mapping = {"sales_data":RemoveNullRows_node1746521434752}, transformation_ctx = "UnitofMeasuresUnique_node1746536048230")

# Script generated for node Distributors - Unique
SqlQuery48 = '''
select distinct(distributor_name) from sales_data
'''
DistributorsUnique_node1746522782479 = sparkSqlQuery(glueContext, query = SqlQuery48, mapping = {"sales_data":RemoveNullRows_node1746521434752}, transformation_ctx = "DistributorsUnique_node1746522782479")

# Script generated for node Warehouses - Unique
SqlQuery49 = '''
select distinct(dc_number), dc_name from sales_data
'''
WarehousesUnique_node1746522833097 = sparkSqlQuery(glueContext, query = SqlQuery49, mapping = {"sales_data":RemoveNullRows_node1746521434752}, transformation_ctx = "WarehousesUnique_node1746522833097")

# Script generated for node New Products
ProductsUnique_node1746523061951DF = ProductsUnique_node1746523061951.toDF()
RDSProducts_node1746559845938DF = RDSProducts_node1746559845938.toDF()
NewProducts_node1746559875659 = DynamicFrame.fromDF(ProductsUnique_node1746523061951DF.join(RDSProducts_node1746559845938DF, (ProductsUnique_node1746523061951DF['distributor_sku'] == RDSProducts_node1746559845938DF['distributor_sku']) & (ProductsUnique_node1746523061951DF['shell_sku_number'] == RDSProducts_node1746559845938DF['shell_sku_number']), "leftanti"), glueContext, "NewProducts_node1746559875659")

# Script generated for node New Units of Measure
UnitofMeasuresUnique_node1746536048230DF = UnitofMeasuresUnique_node1746536048230.toDF()
RDSUnitofMeasures_node1746559628503DF = RDSUnitofMeasures_node1746559628503.toDF()
NewUnitsofMeasure_node1746559656258 = DynamicFrame.fromDF(UnitofMeasuresUnique_node1746536048230DF.join(RDSUnitofMeasures_node1746559628503DF, (UnitofMeasuresUnique_node1746536048230DF['unit_of_measure'] == RDSUnitofMeasures_node1746559628503DF['unit_of_measure']), "leftanti"), glueContext, "NewUnitsofMeasure_node1746559656258")

# Script generated for node New Distributors
DistributorsUnique_node1746522782479DF = DistributorsUnique_node1746522782479.toDF()
RDSDistributors_node1746558730068DF = RDSDistributors_node1746558730068.toDF()
NewDistributors_node1746559182234 = DynamicFrame.fromDF(DistributorsUnique_node1746522782479DF.join(RDSDistributors_node1746558730068DF, (DistributorsUnique_node1746522782479DF['distributor_name'] == RDSDistributors_node1746558730068DF['distributor_name']), "leftanti"), glueContext, "NewDistributors_node1746559182234")

# Script generated for node New Warehouses
WarehousesUnique_node1746522833097DF = WarehousesUnique_node1746522833097.toDF()
RDSWarehouses_node1746559271387DF = RDSWarehouses_node1746559271387.toDF()
NewWarehouses_node1746559287510 = DynamicFrame.fromDF(WarehousesUnique_node1746522833097DF.join(RDSWarehouses_node1746559271387DF, (WarehousesUnique_node1746522833097DF['dc_number'] == RDSWarehouses_node1746559271387DF['dc_number']), "leftanti"), glueContext, "NewWarehouses_node1746559287510")

# Script generated for node RDS - Products
RDSProducts_node1746538048974 = glueContext.write_dynamic_frame.from_catalog(frame=NewProducts_node1746559875659, database="sales-data-pipeline-etl-db", table_name="postgres_public_products", transformation_ctx="RDSProducts_node1746538048974")

# Script generated for node RDS - Unit of Measures
RDSUnitofMeasures_node1746538095357 = glueContext.write_dynamic_frame.from_catalog(frame=NewUnitsofMeasure_node1746559656258, database="sales-data-pipeline-etl-db", table_name="postgres_public_units_of_measures", transformation_ctx="RDSUnitofMeasures_node1746538095357")

# Script generated for node RDS - Distributors
RDSDistributors_node1746535751008 = glueContext.write_dynamic_frame.from_catalog(frame=NewDistributors_node1746559182234, database="sales-data-pipeline-etl-db", table_name="postgres_public_distributors", transformation_ctx="RDSDistributors_node1746535751008")

# Script generated for node RDS - Warehouses
RDSWarehouses_node1746538079865 = glueContext.write_dynamic_frame.from_catalog(frame=NewWarehouses_node1746559287510, database="sales-data-pipeline-etl-db", table_name="postgres_public_warehouses", transformation_ctx="RDSWarehouses_node1746538079865")


### Create Sales Data
# Script generated for node RDS - Warehouses
RDSWarehouses = glueContext.create_dynamic_frame.from_catalog(database="sales-data-pipeline-etl-db", table_name="postgres_public_warehouses", transformation_ctx="RDSWarehouses")

# Script generated for node RDS - Products
RDSProducts = glueContext.create_dynamic_frame.from_catalog(database="sales-data-pipeline-etl-db", table_name="postgres_public_products", transformation_ctx="RDSProducts")

# Script generated for node RDS - Distributors
RDSDistributors = glueContext.create_dynamic_frame.from_catalog(database="sales-data-pipeline-etl-db", table_name="postgres_public_distributors", transformation_ctx="RDSDistributors")

# Script generated for node RDS - Unit of Measures
RDSUnitofMeasures = glueContext.create_dynamic_frame.from_catalog(database="sales-data-pipeline-etl-db", table_name="postgres_public_units_of_measures", transformation_ctx="RDSUnitofMeasures")

# Script generated for node Merged Sales Data
SqlQuery47 = '''
SELECT d.id as distributor_id, w.id as warehouse_id, p.id as product_id, u.id as units_of_measures_id, 
to_date(concat(s.sales_year, lpad(s.sales_month, 2, "0"), s.sales_day), 'yyyyMMdd') as sales_date, 
s.sales_day, s.sales_month, s.sales_year, s.dfoa_quantity, s.non_dfoa_quantity
    FROM sales_data s
    JOIN warehouses w
    JOIN products p
    JOIN unit_of_measures u
    JOIN distributors d ON 
        s.distributor_name = d.distributor_name AND
        s.dc_number = w.dc_number AND
        s.distributor_sku = p.distributor_sku AND
        s.unit_of_measure = u.unit_of_measure
'''
MergedSalesData_node1746561529408 = sparkSqlQuery(glueContext, query = SqlQuery47, mapping = {"sales_data":RemoveNullRows_node1746521434752, "distributors":RDSDistributors, "warehouses":RDSWarehouses, "products":RDSProducts, "unit_of_measures":RDSUnitofMeasures}, transformation_ctx = "MergedSalesData_node1746561529408")

# Script generated for node RDS - Sales
RDSSales_node1746563707337 = glueContext.write_dynamic_frame.from_catalog(frame=MergedSalesData_node1746561529408, database="sales-data-pipeline-etl-db", table_name="postgres_public_sales", transformation_ctx="RDSSales_node1746563707337")

job.commit()