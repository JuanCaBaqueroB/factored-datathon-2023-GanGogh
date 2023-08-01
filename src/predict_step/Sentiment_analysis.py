# Databricks notebook source
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# COMMAND ----------

# MAGIC %md NLTK analysis

# COMMAND ----------

import nltk
nltk.download('punkt')
nltk.download('stopwords')
from nltk.corpus import stopwords

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from reviews;

# COMMAND ----------

# MAGIC %md Model consumption on Generative AI model to perform sentiment analysis

# COMMAND ----------

import urllib.request
import json
import os
import ssl

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
comment="Looks good fits well."
data = {"inputs": {"input_signature":[comment]}}
#my_list = list(data)

body = str.encode(json.dumps(data))

url = 'https://ml-factored-datathon-test-sent.eastus.inference.ml.azure.com/score'
# Replace this with the primary/secondary key or AMLToken for the endpoint
api_key = 'GTVmnjKYcVt8tdMpkRNLtBeSZ5XtV2eg'
if not api_key:
    raise Exception("A key should be provided to invoke the endpoint")

# The azureml-model-deployment header will force the request to go to a specific deployment.
# Remove this header to have the request observe the endpoint traffic rules
headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key), 'azureml-model-deployment': 'finiteautomata-bertweet-base-sen' }

req = urllib.request.Request(url, body, headers)

try:
    response = urllib.request.urlopen(req)

    result = response.read()
    print(result)
except urllib.error.HTTPError as error:
    print("The request failed with status code: " + str(error.code))

    # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
    print(error.info())
    print(error.read().decode("utf8", 'ignore'))

# COMMAND ----------

data = {'Looks good fits well.'}
my_list = list(data)
my_list

# COMMAND ----------

data

# COMMAND ----------

