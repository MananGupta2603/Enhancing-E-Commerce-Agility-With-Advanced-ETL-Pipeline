
# uncommand this code before using

# # Importing the required modules
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from pyspark.context import SparkContext
# from pyspark.sql import functions as F
# from pyspark.sql.utils import AnalysisException

# # Creating a Spark session
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# # Properties of the RDS table to connect
# jdbc_url = "jdbc:mysql://database-1.clm426oqsklj.us-east-1.rds.amazonaws.com:3306/final_project"
# db_properties = {
#     "user": "admin",
#     "password": "Manan123",
#     "driver": "com.mysql.jdbc.Driver"
# }

# # Transforming the data to maintain standard and uniformity
# def clean_headers(df):
#     """
#     Cleans and standardizes column headers:
#     - Sets the first row as the header
#     - Replaces spaces in column names with underscores
#     """
#     try:
#         # Set the first row as header
#         columns = [col.replace(" ", "_") for col in df.columns]
#         df = df.toDF(*columns)
#         return df
#     except Exception as e:
#         print(f"Error cleaning headers: {e}")
#         raise e

# # Retrieving the data from Glue Catalog
# try:
#     orders_df = glueContext.create_dynamic_frame.from_catalog(
#         database="ecommerce_data",
#         table_name="order_bucket"
#     ).toDF()

#     returns_df = glueContext.create_dynamic_frame.from_catalog(
#         database="ecommerce_data",
#         table_name="return_bucket"
#     ).toDF()
# except AnalysisException as e:
#     print(f"Error retrieving data from Glue catalog: {e}")
#     raise e

# # Cleaning the headers of the dataframes
# orders_df = clean_headers(orders_df)
# returns_df = clean_headers(returns_df)

# # Joining the data based on the Order_ID column
# try:
#     joined_df = orders_df.join(
#         returns_df,
#         orders_df.order_id == returns_df.order_id,
#         how="left"
#     ).drop(returns_df.order_id)

#     # Ensuring data types are consistent
#     joined_df = joined_df.withColumn("order_id", F.col("order_id").cast("string"))
# except Exception as e:
#     print(f"Error during join operation: {e}")
#     raise e

# # Inserting the joined dataframe into the RDS table
# try:
#     joined_df.write.jdbc(
#         url=jdbc_url,
#         table="result",
#         mode="overwrite",
#         properties=db_properties
#     )
#     print("Data successfully written to RDS table.")
# except Exception as e:
#     print(f"Error writing to RDS table: {e}")
#     raise e
