# Run Locally
`cd docker`

`docker-compose build --no-cache`

`docker-compose up`
# Proof of Work
#### You should see logs in the console

agent         | Send `{"accelerometer": {"x": -74, "y": 25, "z": 16553}, "gps": {"longitude": 50.45366497455471, "latitude": 30.521629573648056}, "parking": {"empty_count": 2.0, "gps": {"longitude": 50.45246014505101, "latitude": 30.52217026274093}}, "time": "2024-03-03T16:09:19.130868"}` to topic `agent_data_topic`