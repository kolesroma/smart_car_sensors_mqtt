import logging
from typing import List

from fastapi import FastAPI
from redis import Redis

from app.adapters.store_api_adapter import StoreApiAdapter
from app.entities.processed_agent_data import ProcessedAgentData
from config import STORE_API_BASE_URL, REDIS_HOST, REDIS_PORT, BATCH_SIZE

# Configure logging settings
logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO (you can use logging.DEBUG for more detailed logs)
    format="[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s",
    handlers=[
        logging.StreamHandler(),  # Output log messages to the console
        logging.FileHandler("app.log"),  # Save log messages to a file
    ],
)

# Create an instance of the Redis using the configuration
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT)
# Create an instance of the StoreApiAdapter using the configuration
store_adapter = StoreApiAdapter(api_base_url=STORE_API_BASE_URL)

# FastAPI
app = FastAPI()


@app.post("/processed_agent_data/")
async def save_processed_agent_data(processed_agent_data: ProcessedAgentData):
    json_processed_agent_data = processed_agent_data.json()
    redis_client.lpush("processed_agent_data", json_processed_agent_data)

    print("LEN")
    print(redis_client.llen("processed_agent_data"))

    if redis_client.llen("processed_agent_data") >= BATCH_SIZE:
        processed_agent_data_batch: List[ProcessedAgentData] = []
        for _ in range(BATCH_SIZE):
            json_str = redis_client.lpop("processed_agent_data").decode("utf-8")
            processed_agent_data = ProcessedAgentData.parse_raw(json_str)
            processed_agent_data_batch.append(processed_agent_data)

        json_processed_agent_data_batch = [data.json() for data in processed_agent_data_batch]
        print("json_processed_agent_data_batch")
        print(json_processed_agent_data_batch)
        store_adapter.save_data(processed_agent_data_batch=json_processed_agent_data_batch)

    return {"status": "ok"}


@app.post("/save-one/")
async def save_processed_agent_data(processed_agent_data: ProcessedAgentData):
    print("SAVED")
    data_as_dict = [processed_agent_data.dict()]
    store_adapter.save_data(data_as_dict)
    return {"status": "ok"}


@app.get("/redis_keys/")
async def get_redis():
    keys = redis_client.keys()
    print(keys)
    return keys
