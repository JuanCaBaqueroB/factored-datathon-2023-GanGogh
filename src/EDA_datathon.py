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

# MAGIC %md ¿Cómo se comportan estas reviews en el tiempo? 

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date(from_unixtime(unixReviewTime, 'dd/MM/yyyy'),"dd/MM/yyyy") as date, count(reviewerID)
# MAGIC FROM reviews
# MAGIC where verified="false"
# MAGIC group by date
# MAGIC order by date;

# COMMAND ----------

dt = DeltaTable("resources/delta/1")
df = dt.to_pandas()
print(df)