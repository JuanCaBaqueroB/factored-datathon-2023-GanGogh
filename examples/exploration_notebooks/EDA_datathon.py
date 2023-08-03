# Databricks notebook source
# MAGIC %md
# MAGIC EDA - Reviews

# COMMAND ----------

pip install deltalake

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT *) FROM reviews;
# MAGIC --SELECT * FROM reviews limit 10;
# MAGIC --SELECT * FROM products_metadata limit 10;
# MAGIC --SHOW COLUMNS IN products_metadata;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM reviews;

# COMMAND ----------

# MAGIC %md ¿Cómo se comportan los reviews?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT overall FROM reviews;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ¿Cómo se comportan los reviews en el tiempo? 

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date(from_unixtime(unixReviewTime, 'MM/yyyy'),"MM/yyyy") as date, avg(overall)
# MAGIC FROM reviews
# MAGIC where verified="true"
# MAGIC group by date
# MAGIC order by date;

# COMMAND ----------

#¿Cómo se comportan las reviews en el tiempo?
%sql
select to_date(from_unixtime(unixReviewTime, 'MM/yyyy'),"MM/yyyy") as date, count(reviewerID)
FROM reviews
where verified="false"
group by date
order by date;

# COMMAND ----------

# MAGIC %md Verified reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT overall FROM reviews
# MAGIC WHERE verified="true";

# COMMAND ----------

# MAGIC %md Join between reviews and product data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reviews_x_product LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select asin_2,ASIN_25,ASIN_26 from products_metadata;
# MAGIC --select count(*) from products_metadata; 
# MAGIC --where AgeRange_28 is not null 
# MAGIC --LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE reviews_x_product
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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE summary_score_reviews AS
# MAGIC select to_date(from_unixtime(unixReviewTime, 'MM/yyyy'),"MM/yyyy") as date, maincat_10, verified, ROUND(avg(overall),2) as Average, min(overall) as Min_rate, max(overall) as Max_rate, ROUND(sum(overall)/(5*count(overall)),2) as efficiency_rate, count(overall) as Qty, ROUND(stddev(overall),2) as sDeviation
# MAGIC FROM reviews_x_product
# MAGIC group by date,maincat_10, verified
# MAGIC order by date;

# COMMAND ----------

