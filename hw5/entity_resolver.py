import json
import asyncio
import requests
from confluent_kafka import Consumer, Producer
from cassandra.cluster import Cluster
import uuid

# Kafka (Redpanda) connection settings
redpanda_host = "localhost"
redpanda_port = 19092
input_topic = "address_input"
output_topic = "resolved_addresses"

# Quine ingest stream name and API URL
ingest_stream_name = "address-ingest"
quine_url = "http://localhost:8088/api/v1"
quine_ingest_url = f"{quine_url}/ingest/{ingest_stream_name}"

# Kafka SASL settings
kafka_common_config = {
    "bootstrap.servers": f"{redpanda_host}:{redpanda_port}",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "superuser",
    "sasl.password": "secretpassword"
}

producer = Producer(kafka_common_config)
consumer = Consumer({
    **kafka_common_config,
    "group.id": "address-processor",
    "auto.offset.reset": "earliest"
})

# Cassandra connection settings
cassandra_host = 'localhost'
cassandra_port = 9042
cassandra_keyspace = 'address_resolution'

# Create Cassandra session
def create_cassandra_session():
    cluster = Cluster([cassandra_host], port=cassandra_port)
    session = cluster.connect(cassandra_keyspace)
    return session

cassandra_session = create_cassandra_session()

# Load CypherQL queries from file
def read_cypher_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Check if Quine ingest stream already exists
def check_if_stream_exists():
    url = f"{quine_url}/ingest/{ingest_stream_name}"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Ingest stream '{ingest_stream_name}' already exists.")
        return True
    else:
        print(f"Ingest stream '{ingest_stream_name}' does not exist.")
        return False

# Create the ingest stream on Quine 
def create_ingest_stream():
    if not check_if_stream_exists():
        # Define the Quine stream ingest config
        stream_config = {
            "type": "KafkaIngest", 
            "topics": [input_topic], 
            "bootstrapServers": f"{redpanda_host}:{redpanda_port}",
            "groupId": "quine-consumer-group", 
            "format": {
                "type": "CypherJson",
                "query": "MATCH (n:Address) RETURN n"  
            }
        }

        try:
            response = requests.post(f"{quine_url}/api/v1/ingest/{ingest_stream_name}", json=stream_config)
            response.raise_for_status() 
            print(f"Ingest stream '{ingest_stream_name}' created successfully.")
        except requests.RequestException as e:
            print(f"Failed to create ingest stream: {e}")
            if e.response is not None:
                print("Response text:", e.response.text)

# Send JSON payload to Quine
def ingest_to_quine(payload):
    try:
        quine_payload = {
            "type": "KafkaIngest",  # Include Kafka ingest type
            "topics": [input_topic],  # Kafka topic
            "bootstrapServers": f"{redpanda_host}:{redpanda_port}",  # Redpanda (Kafka) servers
            "addressee": payload["addressee"],
            "original": payload["original"],
            "city": payload["city"],
            "state": payload["state"],
            "house": payload.get("house", "unknown"),
            "houseNumber": payload.get("houseNumber", "unknown"),
            "postcode": payload.get("postcode", "unknown"),
            "road": payload.get("road", "unknown")
        }

        response = requests.post(quine_ingest_url, json=quine_payload)
        response.raise_for_status()
        print("Ingested to Quine:", quine_payload)
    except requests.RequestException as e:
        print("Quine ingest failed:", e)
        if e.response is not None:
            print("Response text:", e.response.text)

# Insert resolved data into Cassandra
def insert_into_cassandra(resolved_entity):
    try:
        query = """
        INSERT INTO resolved_entities (resolved_id, addressee, original, city, house, houseNumber, postcode, road, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cassandra_session.execute(query, (
            uuid.UUID(resolved_entity['resolved']),
            resolved_entity['addressee'],
            resolved_entity['original'],
            resolved_entity['parts']['city'],
            resolved_entity['parts']['house'],
            resolved_entity['parts']['houseNumber'],
            resolved_entity['parts']['postcode'],
            resolved_entity['parts']['road'],
            resolved_entity['parts']['state']
        ))
        print(f"Inserted resolved entity into Cassandra: {resolved_entity}")
    except Exception as e:
        print(f"Error inserting into Cassandra: {e}")

# Process incoming Kafka message
async def process_data(message, counter):
    try:
        raw_value = message.value().decode("utf-8")
        data = json.loads(raw_value)

        print(f"Received message: {data}")

        # Prepare resolved entity
        resolved_entity = {
            "addressee": data.get("addressee", "unknown"),
            "original": data.get("original", "unknown address"),
            "parts": {
                "city": data.get("city", "unknown"),
                "house": data.get("house", "unknown"),
                "houseNumber": data.get("houseNumber"),
                "postcode": data.get("postcode", "unknown"),
                "road": data.get("road", "unknown"),
                "state": data.get("state", "unknown")
            },
            "resolved": str(uuid.uuid4())  # Generate a new resolved UUID
        }

        # Insert into Cassandra
        insert_into_cassandra(resolved_entity)

        # Ingest to Quine
        quine_payload = {
            "type": "KafkaIngest",
            "topics": [input_topic],
            "bootstrapServers": f"{redpanda_host}:{redpanda_port}",
            "addressee": resolved_entity["addressee"],
            "original": resolved_entity["original"],
            "poBox": data.get("poBox"),
            "postcode": data.get("postcode"),
            "city": data.get("city"),
            "state": data.get("state")
        }
        ingest_to_quine(quine_payload)

        # Send to output Redpanda topic
        producer.produce(output_topic, json.dumps(quine_payload).encode("utf-8"))
        producer.flush() 

        # Increment counter
        counter += 1
        print(f"Processed message count: {counter}")

    except Exception as e:
        print(f"Error processing message: {e}")
    return counter

# Main loop to consume Redpanda and ingest to Quine
async def main():
    # Create the ingest stream once at the start
    create_ingest_stream()

    consumer.subscribe([input_topic])
    print("Listening for messages on:", input_topic)

    # Initialize message counter
    message_counter = 0

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            await asyncio.sleep(1)
            continue
        elif msg.error():
            print(f"Consumer error: {msg.error()}")
        else:
            message_counter = await process_data(msg, message_counter)

if __name__ == "__main__":
    asyncio.run(main())



