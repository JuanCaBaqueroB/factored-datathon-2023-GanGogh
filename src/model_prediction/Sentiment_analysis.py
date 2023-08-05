# Databricks notebook source
# MAGIC %md Model consumption on generative AI model to perform sentiment analysis

# COMMAND ----------

import urllib.request
import json
import os
import ssl
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

key_vault_name = "mlfactoreddata3978247496"
keyvault_uri = f"https://{key_vault_name}.vault.azure.net"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=keyvault_uri, credential=credential)


def allowSelfSignedHttps(allowed):
    # bypass the server certificate verification on client side
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '')\
        and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True)
# this line is needed if you use self-signed certificate in your scoring service

# Request data goes here
# The example below assumes JSON formatting which may be updated
# depending on the format your endpoint expects.

# Data consumption example
comment="Looks good fits well."
comment2="Looks bad."
comment3="I don't have opinions"
inputList = [comment,comment2,comment3]
data = {"inputs": {"input_signature":inputList}}
#print(type([comment,comment2,comment3]))
body = str.encode(json.dumps(data))

url='https://ml-factored-datathon-test-sent.eastus.inference.ml.azure.com/score'
# Replace this with the primary/secondary key or AMLToken for the endpoint
api_key = client.get_secret("SENTIMENT-API-KEY")
if not api_key:
    raise Exception("A key should be provided to invoke the endpoint")

# The azureml-model-deployment header will force the request to go to deployment
# Remove this header to have the request observe the endpoint traffic rules
headers = {'Content-Type':'application/json',
            'Authorization':('Bearer '+ api_key),
            'azureml-model-deployment': 'finiteautomata-bertweet-base-sen'
          }

def get_sentiment(inputData):
    #inputData=[comment,comment2,comment3]
    data = {"inputs": {"input_signature":inputData}}
    body = str.encode(json.dumps(data))
    req = urllib.request.Request(url, body, headers)

    try:
        response = urllib.request.urlopen(req)

        result = response.read()
        return result
        #print(result)
    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp,
        # which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))

# COMMAND ----------

#Load table on dataframe
df_rev = spark.read.format("delta").load("dbfs:/user/hive/warehouse/reviews")
df_rev.count()

# COMMAND ----------

# Access the first 100 rows (rows with ID from 1 to 100)
df_n_rows = df_rev.filter(col("ID").between(1000,1100)).select("ID",
                                                                "reviewText")
#display(df_rev)

#Convert spark DF column to list
dfReview = df_n_rows.select('reviewText').rdd.flatMap(lambda x: x).collect()
dfId = df_n_rows.select('ID').rdd.flatMap(lambda x: x).collect()

#Sent to ML service
result = get_sentiment(dfReview)

if result is not None:
    decoded_result = result.decode()
    # Your code to process the decoded result goes here
else:
    # Handle the case when 'result' is None
    print("Result is None, no decoding needed.")

# Convert the string to a list of dictionaries
output = ast.literal_eval(decoded_result)
# Convert to a list of values
output_list = [item['0'] for item in output]
dfList=[dfId,output_list]

# Transpose the list using zip
transposed_list = list(map(list, zip(*dfList)))

# Print the resulting list
print(len(transposed_list))

# COMMAND ----------

# Assuming you already have a SparkSession called 'spark' and a list called
# 'data_list' 
# giving column names of dataframe
columns = ["ID2","Sentiment"]

# creating a dataframe
df_sentiment = spark.createDataFrame(transposed_list, columns)
  
# show data frame
# dataframe.show()

# Union the DataFrame with the Delta table
updated_df = df_rev.join(df_sentiment, df_rev.ID==df_sentiment.ID2).drop("ID2")
display(updated_df.limit(5))

# Write the updated DataFrame back to the Delta table
# updated_df.write.format("delta").mode("overwrite").save("your_delta_table")

# COMMAND ----------

# Assuming you have a Spark DataFrame called 'spark_df'

# Define a function to be applied to each partition
def process_partition(iterator):
    # Replace 'column_name' with the actual name of th column you want to access
    for row in iterator:
        value = row['summary']
        # Do some processing on the value (e.g., print it)
        print(value)

# Apply the function to each partition
dfrev.foreachPartition(process_partition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT asin, overall, summary, reviewerID, verified,to_date(from_unixtime(unixReviewTime, 'MM/yyyy'),"MM/yyyy") as date
# MAGIC from reviews_x_product order by date limit 10;