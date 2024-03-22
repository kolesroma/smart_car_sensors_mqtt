import json

import requests

from app.interfaces.hub_gateway import HubGateway


class HubHttpAdapter(HubGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch):
        url = f"http://store:8000/processed_agent_data/"
        print(url)
        json_processed_agent_data_batch = [json.loads(data) for data in processed_agent_data_batch]
        response = requests.post(url, json=json_processed_agent_data_batch)
        return response
