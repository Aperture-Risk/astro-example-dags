"""
Copyright 2021 Bloomberg L.P. All rights reserved.

Use governed by applicable Bloomberg Terminal agreement.

"""
import json
import random
import time

from requests.exceptions import ChunkedEncodingError

import requests
import json
import jwt
import uuid
import binascii
import datetime
import asyncio

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime



endpoint = "https://api.bloomberg.com"

ID = {"client_id":"8b3c4f8444916ddc10c623a868ffd948","client_secret":"75bbae79f7269190326ef3ce3aca59a74c675799ccc4970acfd5db6682b0a6da","name":"ApertureIB","scopes":["ib"],"expiration_date":1758461207565,"created_date":1711027607565}
client_id = ID['client_id']
client_secret = ID['client_secret']


def generate_jwt(uri, path, method):
    """
    Generates a JWT from the URL and ID, and secret.
    See authentication_documentation/jwt.md for details
    """

    # Get the current time in seconds since the epoc
    current_time = int(round(time.time()))

    # This is json encoded, then stored in the body of the JWT
    body = {
        "iss": client_id,
        "exp": int(current_time + 300),  # expire time, 5 minutes from now
        # We substract 10 seconds from these values in case our clock is
        # not well synchronized
        "nbf": int(current_time - 60),   # not-before time
        "iat": int(current_time - 60),   # issued-ad time
        "region": "default",  # 'default' is the only allowed value for region
        "method": method,
        "path": path,
        "host": uri,
        # nonce, this could be any random string
        "jti": str(uuid.uuid4()),
    }

    # Encode the JWT using the body, and hash it with the client secret.
    # Use the HS256 HMAC algorithm. HS384 or HS512 could also be used.
    # If you are re-implementing this in some other language other than python
    # use your languages JWT function, or find a popular library. For example
    # node-jsonwebtoken for node.js, java-jwt for java, or jwt-dotnet for .NET
    return jwt.encode(body,
                      # Convert the hex client_secret to binary data
                      binascii.unhexlify(client_secret),
                      algorithm='HS256')


def generate_url(uri, path, method):
    """
    This function generates the JWT, and appends it onto the URL with
    a jwt= query parameter.
    """
    return (uri + path + "?jwt=" +
            generate_jwt(uri, path, method))


def get(path, stream=False):
    uri_with_jwt = generate_url(endpoint, path, "GET")

    response = requests.get(uri_with_jwt, "GET", stream=stream)
    if response.status_code not in range(200, 300):
        print("Response body: {}".format(response.text))
        raise Exception("Post returned error status: {} - {}"
                        .format(response.status_code,
                                response.reason))
    else:
        return response
    
async def fetch(session, url):
    while True:
        async with session.get(url) as response:
            reader = response.content

            cnt = 0
            async for line in reader:
                cnt += 1

            print(f"{url}: {cnt} lines read")

        await asyncio.sleep(3)


async def run(msg):
    from azure.eventhub import EventData
    from azure.eventhub.aio import EventHubProducerClient
    import json
    import os
    # create a producer client to send messages to the event hub
    # specify connection string to your event hubs namespace and
    # the event hub name
    producer = EventHubProducerClient.from_connection_string(conn_str=os.getenv("STRING"), eventhub_name=os.getenv("HUB_KEY"))
    async with producer:
        # create a batch
        event_data_batch = await producer.create_batch()

        # add events to the batch
        event_data_batch.add(EventData(json.dumps(msg)))
        # send the batch of events to the event hub
        await producer.send_batch(event_data_batch)


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["IB Connect"], 
)
def bloomberg_ib_connect_stream():

    @task
    def stream_data():
       
        # Or check if a time is between two other times
        end = datetime.time(11, 20)
        streams = json.loads(get("/ib/v1/streams").text)
        first_stream = streams['streams'][0]

        print(first_stream)

        print("/ib/v1/streams/{}".format(first_stream['id']))   

        max_backoff_second = 2**4  # You can set it to any value that fits your workflow
        backoff_second = 1
        while True:
            try:
                response = get("/ib/v1/streams/{}".format(first_stream['id']), stream=True)
                
                it = response.iter_lines(decode_unicode=True, chunk_size=1)

                print("Start listening to the stream...")
                backoff_second = 1  # reset backoff time

                for chunk in it:
                    loaded_msg = json.loads(chunk)
                    if not loaded_msg:  # heartbeat
                        continue
                    print(loaded_msg)
                
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(run(loaded_msg))
                    print("sent to azure successfully")
                    timestamp = datetime.datetime.now().time()
                    if timestamp > end:
                        print("ENDING PROCESS...............")
                        exit()

            except ChunkedEncodingError as e:
                print(f"ChunkedEncodingError happened {e}")
            except Exception as e:
                print(f"Exception happened {e}")

            # Exponential backoff: the sleep time is capped by max_backoff_second
            sleep_time = backoff_second + random.uniform(0, 0.5)
            print(f"Trying again in {sleep_time} seconds...")
            time.sleep(sleep_time)
            backoff_second = min(backoff_second * 2, max_backoff_second)

    stream = stream_data()


bloomberg_ib_connect_stream()