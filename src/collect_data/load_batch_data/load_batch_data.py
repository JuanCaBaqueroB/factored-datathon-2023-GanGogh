import itertools
from pyspark.sql import functions as func
from pyspark.sql import DataFrame
from typing import List
import re

storage_account = ""

AZURE_SERVER_CORE = "dfs.core.windows.net"
AUTH_TYPE_ADLS = f"fs.azure.account.auth.type.{storage_account}"\
				 + f".{AZURE_SERVER_CORE}"
AUTH_PROVIDER_ADLS = f"fs.azure.sas.token.provider.type.{storage_account}"\
					 + f".{AZURE_SERVER_CORE}"
AUTH_TYPE_ADLS = "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
SET_SAS_TOKEN = f"fs.azure.sas.fixed.token.{storage_account}"\
				+ f".{AZURE_SERVER_CORE}"
ADLS_METADATA_PATH = f"abfss://{container_name}@{storage_account}"\
					 + f".{AZURE_SERVER_CORE}/{path_metadata}"
ADLS_REVIEWS_PATH = f"abfss://{container_name}@{storage_account}"\
					+ f".{AZURE_SERVER_CORE}/{path_reviews}"

spark.conf.set(AUTH_TYPE_ADLS, "SAS")
spark.conf.set(AUTH_PROVIDER_ADLS, AUTH_TYPE_ADLS)
spark.conf.set(SET_SAS_TOKEN, sas_token_key)



def get_input_dataframe(list_files: List) -> DataFrame:
	"""Get a merged DataFrame with all the input json files provided in the
	input list.
	Args:
	    list_files (List): ADLS location of each json file to be merged.
	Returns:
	    DataFrame: All input data found in json files merged into a table.
	"""
	complete_json_list = [[some_file.path\
							for some_file in dbutils.fs.ls(file_listed.path)\
	                        if 'json.gz' in some_file.path
	                      ]\
	                      for file_listed in list_files
	                     ]
	complete_json_list = list(itertools.chain(*complete_json_list))
	return spark.read.json(complete_json_list)\
						.withColumn("source_file", func.input_file_name())



def format_metadata_table(df_metadata: DataFrame) -> DataFrame:
	"""Format the products-metadata DataFrame, by breaking the struct details
	field and renaming the columns to avoid special characters, not allowed
	in Delta Tables.
	Args:
	    df_metadata (DataFrame): Products-metadata table.
	Returns:
	    DataFrame: Products-metadata table with a modified schema.
	"""
	df_metadata = df_metadata.select('*', func.col('details.*')).drop('details')
	for i_col, each_col in enumerate(df_metadata.columns):
	    new_name = re.sub('[^A-Za-z0-9]+', '', each_col) + f'_{i_col}'
	    df_metadata = df_metadata.withColumnRenamed(each_col, new_name)
	return df_metadata


list_files_metadata = dbutils.fs.ls(ADLS_METADATA_PATH)
list_files_review = dbutils.fs.ls(ADLS_REVIEWS_PATH)

df_metadata = get_input_dataframe(list_files_metadata)
df_metadata = format_metadata_table(df_metadata)


df_reviews = get_input_dataframe(list_files_review)


df_metadata.write.format('delta').option("delta.columnMapping.mode", "name")\
					.saveAsTable('products_metadata')
df_reviews.write.saveAsTable('reviews')
