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

def allowSelfSignedHttps(allowed):
    # bypass the server certificate verification on client side
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True) # this line is needed if you use self-signed certificate in your scoring service.

# Request data goes here
# The example below assumes JSON formatting which may be updated
# depending on the format your endpoint expects.
# More information can be found here:
# https://docs.microsoft.com/azure/machine-learning/how-to-deploy-advanced-entry-script
# Data consumption example
comment="Looks good fits well."
comment2="Looks bad."
comment3="I don't have opinions"
inputList = [comment,comment2,comment3]
data = {"inputs": {"input_signature":inputList}}
#print(type([comment,comment2,comment3]))
body = str.encode(json.dumps(data))

url = 'https://ml-factored-datathon-test-sent.eastus.inference.ml.azure.com/score'
# Replace this with the primary/secondary key or AMLToken for the endpoint
api_key = 'GTVmnjKYcVt8tdMpkRNLtBeSZ5XtV2eg'
if not api_key:
    raise Exception("A key should be provided to invoke the endpoint")

# The azureml-model-deployment header will force the request to go to a specific deployment.
# Remove this header to have the request observe the endpoint traffic rules
headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key), 'azureml-model-deployment': 'finiteautomata-bertweet-base-sen' }

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

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))

# COMMAND ----------

#Load table on dataframe
df_rev = spark.read.format("delta").load("dbfs:/user/hive/warehouse/reviews")
df_rev.count()

# COMMAND ----------

# Access the first n rows (rows with ID from 1 to n)
#df_n_rows = df_rev.filter(col("ID").between(1,300)).select("ID", "reviewText")
#display(df_rev)

def get_chunk_sentiment(df_chunk):
    #Convert spark DF column to list
    dfReview = df_chunk.select('reviewText').rdd.flatMap(lambda x: x).collect()
    dfId = df_chunk.select('ID').rdd.flatMap(lambda x: x).collect()

    #Sent to ML service
    result = get_sentiment(dfReview)

    if result is not None:
        decoded_result = result.decode()

        # Convert the string to a list of dictionaries
        output = ast.literal_eval(decoded_result)
        # Convert to a list of values
        output_list = [item['0'] for item in output]
        dfList=[dfId,output_list]

        # Transpose the list using zip
        transposed_list = list(map(list, zip(*dfList)))

        # giving column names of dataframe
        columns = ["ID2","Sentiment"]

        # creating a dataframe
        df_sentiment = spark.createDataFrame(transposed_list, columns)

        # Add sentiment column to original dataframe
        #updated_df_1 = df_rev.join(df_sentiment, df_rev.ID == df_sentiment.ID2).drop("ID2")

        return df_sentiment

    else:
        # Handle the case when 'result' is None
        print("Result is None, no decoding needed.")
        # giving column names of dataframe
        columns = ["ID2","Sentiment"]
        listaError=[['ID','NO RESPONSE']]
        df_sentiment_error = spark.createDataFrame(listaError, columns)       
        return df_sentiment_error



#a=get_chunk_sentiment(df_n_rows)
#display(a)

# COMMAND ----------

"""
columns = ["ID2","Sentiment"]
listaError=[['ID','NO RESPONSE']]
#transposed_list
# creating a dataframe
df_sentiment_error = spark.createDataFrame(listaError, columns)
display(df_sentiment_error)
"""

# COMMAND ----------

# Add sentiment column to original dataframe
#updated_df_1 = df_rev.join(df_sentiment, df_rev.ID == df_sentiment.ID2).drop("ID2")
#display(updated_df_1.limit(5))

# Write the updated DataFrame back to the Delta table
# updated_df.write.format("delta").mode("overwrite").save("your_delta_table")

# COMMAND ----------

# MAGIC %md Calculate sentiment for each review

# COMMAND ----------

display(final_updated_df)

# COMMAND ----------

#isplay("dbfs:/user/hive/warehouse//dataframe_S_1")

df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/dataframe_S_5")
display(df)

# COMMAND ----------

((i-1)*n)+1

# COMMAND ----------

#Code to test the code and bugs
chunk_df=df_rev.filter(col("ID").between(((i-1)*n)+1,n*i)).select("ID", "reviewText")

# Create a new DataFrame with an additional "sentiment" column with value "POS"
updated_df = get_chunk_sentiment(chunk_df)
display(updated_df)

# COMMAND ----------

# Define the number of rows in each batch
n=400

# Define the total number of iterations needed to loop over the reviews table
iters=int(df_rev.count()/n)
#print(iters/n)

# Initialize an empty list to store the updated DataFrames
updated_dfs = []

# Iterate through the chunks of n rows each iters+1
for i in range(1,iters):
    # Take a chunk of n rows from the DataFrame
    print(((i-1)*n)+1,n*i)
    chunk_df=df_rev.filter(col("ID").between(((i-1)*n)+1,n*i)).select("ID", "reviewText")

    # Create a new DataFrame with an additional "sentiment" column
    updated_df = get_chunk_sentiment(chunk_df)

    # Create a dataframe
    """try:
        output_directory="dbfs:/user/hive/warehouse"
        # Save each DataFrame with a unique name based on the loop index
        updated_df.write.format("delta").mode("overwrite").save(f"{output_directory}/dataframe_S_{i}")
        print(f"DataFrame {i} saved successfully.")
        
    except AnalysisException as e:
        print(f"Error saving DataFrame {i}: {e}")
        # Continue the loop even if an error occurs
        continue"""
    
    # Append the updated DataFrame to the list
    updated_dfs.append(updated_df)

# Concatenate all the updated DataFrames
final_updated_df = updated_dfs[0]

for df in updated_dfs[1:]:
    #print(df)
    final_updated_df = final_updated_df.union(df)

# Show the final updated DataFrame
final_updated_df.show()

# COMMAND ----------

# MAGIC %md Create new delta table from reviews with sentiments

# COMMAND ----------

# Add sentiment column to original dataframe - tiempo en ejecución
updated_df = df_rev.join(final_updated_df, df_rev.ID == final_updated_df.ID2).drop("ID2")

# COMMAND ----------

# Create delta table from reviews_w_sentiment with sentiments
#updated_df.write.format("delta").save("dbfs:/user/hive/warehouse/reviews_w_sentiment")

updated_df.write.format("delta").mode("overwrite").saveAsTable('reviews_with_sentiment')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM reviews_with_sentiment limit 5;