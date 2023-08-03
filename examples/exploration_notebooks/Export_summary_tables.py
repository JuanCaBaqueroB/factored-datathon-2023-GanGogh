# Databricks notebook source
# MAGIC %md Create preprocessed tables for visualization

# COMMAND ----------

# MAGIC %md Table reviews_x_product

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE OR REPLACE reviews_x_product
# MAGIC     COMMENT 'View for reviews with product information'
# MAGIC     AS select A.*,
# MAGIC     B.alsobuy_0,
# MAGIC     B.alsoview_1,
# MAGIC     B.asin_2,
# MAGIC     B.brand_3,
# MAGIC     B.category_4,
# MAGIC     B.date_5,
# MAGIC     B.description_6,
# MAGIC     B.feature_7,
# MAGIC     B.fit_8,
# MAGIC     B.image_9,
# MAGIC     B.maincat_10,
# MAGIC     B.price_11,
# MAGIC     B.rank_12,
# MAGIC     B.similaritem_13,
# MAGIC     B.tech1_14,
# MAGIC     B.tech2_15,
# MAGIC     B.title_16,
# MAGIC     B.ItemWeight_17,
# MAGIC     B.PackageDimensions_18,
# MAGIC     B.ProductDimensions_19,
# MAGIC     B.DatefirstlistedonAmazon_20,
# MAGIC     B.UNSPSCCode_21
# MAGIC     FROM reviews A
# MAGIC     inner join products_metadata B
# MAGIC     ON A.asin=B.asin_2;

# COMMAND ----------

# MAGIC %md Table summary_score_reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE summary_score_reviews AS
# MAGIC select to_date(from_unixtime(unixReviewTime, 'MM/yyyy'),"MM/yyyy") as date, maincat_10, verified, ROUND(avg(overall),2) as Average, min(overall) as Min_rate, max(overall) as Max_rate, ROUND(sum(overall)/(5*count(overall)),2) as efficiency_rate, count(overall) as Qty, ROUND(stddev(overall),2) as sDeviation
# MAGIC FROM reviews_x_product
# MAGIC group by date,maincat_10, verified
# MAGIC order by date;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP VIEW reviews_x_product

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd

# Azure Blob Storage credentials
storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=test1fast;AccountKey=QnSkjChqVUQWCLs9t+yDSK4w02oQVBjWtP9dOOBhpw1O002GrWnk8LHfsU8Ys16QjNKmjnDw2RbM+AStEQNjww==;EndpointSuffix=core.windows.net"
container_name = "visualization-tables"

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
container_client = blob_service_client.get_container_client(container_name)
blob_client = container_client.get_blob_client(csv_filename)

# COMMAND ----------

df = pd.read_csv(csv_filename)
df

# COMMAND ----------

#Save csv on Azure Blob Storage
with open("stockPrice.csv", mode="rb") as data:
    blob_client = container_client.upload_blob(name="stockPrice.csv", data=data, overwrite=True)

# COMMAND ----------

