import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import json

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
 	    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://testsparkstreaming.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=z1JAKV+wRRt7KtY+rGAgCkcH5ez5ZuqD9WJmfVkAl9s=", eventhub_name="ingestevents")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()
        

        # Add events to the batch.
        for value in range(0,100000):
            data = {}
            data['metric'] = "{}".format(value + 1)
            json_data = json.dumps(data)
            print("{}".format(json_data))
            event_data_batch.add(EventData("{}".format(json_data)))
            await producer.send_batch(event_data_batch)
        

        # Send the batch of events to the event hub.
        

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
