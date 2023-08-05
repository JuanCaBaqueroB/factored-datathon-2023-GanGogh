from pyspark.sql import functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

key_vault_name = "mlfactoreddata3978247496"
keyvault_uri = f"https://{key_vault_name}.vault.azure.net"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=keyvault_uri, credential=credential)


storage_account = 'test1fast'
access_key = client.get_secret("STORAGE-ACCOUNT-ACCESSKEY")

spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
               f"{access_key}")

container_name = 'streaming-events-captured-factored'

dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account}.blob.core.windows.net",
  mount_point="/mnt/blob_storage",
  extra_configs={
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net":\
    f"{access_key}"
  }
)

list_files_to_ingest = dbutils.fs.ls("/mnt/blob_storage")
all_json_files = [some_file.path for some_file in list_files_to_ingest]

df = spark.read.json(all_json_files)\
                .withColumn("source_file", func.input_file_name())\
                .withColumn("unixReviewTime", func.split(
                                                func.split(
                                                        func.col('source_file'),
                                                        'new_event_'
                                                          )\
                                                .getItem(1),
                                                '.json'
                                                        )\
                                              .getItem(0).cast('int')
                            )

max_id = spark.sql('SELECT MAX(ID) AS MAX_ID FROM reviews')\
              .toPandas()['MAX_ID'][0]
max_time = spark.sql('SELECT MAX(unixReviewTime) AS MAX_ID FROM reviews')\
              .toPandas()['unixReviewTime'][0]

df = df.filter( func.col('unixReviewTime')>max_time )

window_spec = Window.orderBy('partition_number')
df = df.withColumn("ID", func.row_number().over(window_spec) + max_id + 1)

df.select('asin', 'image', 'overall', 'reviewText', 'reviewerID',
          'reviewerName', 'style', 'summary', 'unixReviewTime', 'verified',
          'vote', 'ID')\
  .dropDuplicates().write.format("delta").mode("append").insertInto("reviews")
