from azure.eventhub import EventHubConsumerClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
from datetime import datetime

# Event Hub namespace, topic, and connection details
eventhub_namespace = "factored-datathon"
eventhub_name = "factored_datathon_amazon_reviews_5"
listen_policy_connection_string = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_group_5;SharedAccessKey=bhbWAqLTLi5/DASs+HOCYuwO0Kf5QRS4x+AEhMZauUE=;EntityPath=factored_datathon_amazon_reviews_5"

# Event Hub consumer group
consumer_group = "gangogh"  # You can specify your custom consumer group here

# Azure Blob Storage connection string and container name
storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=test1fast;AccountKey=QnSkjChqVUQWCLs9t+yDSK4w02oQVBjWtP9dOOBhpw1O002GrWnk8LHfsU8Ys16QjNKmjnDw2RbM+AStEQNjww==;EndpointSuffix=core.windows.net"
container_name = "testblob1"

# Batch size for writing events
batch_size = 1000

# Batch list to accumulate events
batched_events = []


# Function to write the batched events to the blob
def write_batch_to_blob():
    # Store the event data in JSON format in Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Generate a unique file name for the batch
    # (You can customize the file name as per your requirement)
    reception_time = int(datetime.utcnow().timestamp())
    blob_name = "batch_events_{}.json".format(reception_time)

    # Create a new blob with the batched events data in JSON format
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(batched_events, overwrite=True)

    # Clear the batch list for the next set of events
    batched_events.clear()


# Callback function to process and store received events
def on_event(partition_context, event):
    # Process the received event data
    received_data = event.body_as_str()
    print(received_data)

    # Add the event data to the batch list
    batched_events.append(received_data)

    # If the batch size is reached, write the batched events to the blob
    if len(batched_events) >= batch_size:
        write_batch_to_blob()

    # Store the event data in JSON format in Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Generate a unique file name for each event
    # Parse the JSON string
    blob_data = json.loads(received_data)

    # Extract the partition number
    partition_number = blob_data.get("partition_number")

    # Print the result
    blob_name = "new_event_{}.json".format(partition_number)

    # Create a new blob with the event data in JSON format
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(received_data, overwrite=True)
    print(f'Blob uploaded as {blob_name}')


# Create an instance of the EventHubConsumerClient
client = EventHubConsumerClient.from_connection_string(
    listen_policy_connection_string,
    consumer_group=consumer_group,
    eventhub_name=eventhub_name
)

try:
    # Receive events from the Event Hub (starting from the latest available position)
    with client:
        client.receive(on_event=on_event)

except KeyboardInterrupt:
    print("Receiving has stopped.")