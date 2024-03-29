import logging
import paho.mqtt.client as mqtt
from app.interfaces.agent_gateway import AgentGateway
from app.entities.agent_data import AgentData
from app.usecases.data_processing import process_agent_data
from app.interfaces.hub_gateway import HubGateway


class AgentMQTTAdapter(AgentGateway):
    def __init__(self, broker_host, broker_port, topic, hub_gateway: HubGateway, batch_size=3):
        self.batch_size = batch_size
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.client = mqtt.Client()
        self.hub_gateway = hub_gateway

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker")
            self.client.subscribe(self.topic)
        else:
            logging.info(f"Failed to connect to MQTT broker with code: {rc}")

    def on_message(self, client, userdata, msg):
        """Processing agent data and sent it to hub gateway"""
        logging.info(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        try:
            payload: str = msg.payload.decode()
            agent_data = AgentData.model_validate_json(payload, strict=True)
            processed_data = process_agent_data(agent_data)

            json_processed_agent_data = processed_data.json()
            json_processed_agent_data_batch = [json_processed_agent_data]

            if not self.hub_gateway.save_data(processed_agent_data_batch=json_processed_agent_data_batch):
                logging.error("Hub is not available")
        except Exception as e:
            logging.info(f"Error processing MQTT message: {e}")

    def connect(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.broker_host, self.broker_port)

    def start(self):
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
