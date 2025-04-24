import json
import asyncio
import requests
import uuid
from uuid import UUID
from confluent_kafka import Consumer, Producer
from cassandra.cluster import Cluster
import threading

# Kafka (Redpanda) connection settings
redpanda_host = "localhost"
redpanda_port = 19092
input_topic = "address_input"
output_topic = "resolved_addresses"

# Quine URL
quine_url = "http://localhost:8088/api/v1"

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

# Cassandra connection settings (no authentication)
cassandra_host = 'localhost'
cassandra_port = 9042
cassandra_keyspace = 'address_resolution'

# Create Cassandra session
def create_cassandra_session():
    cluster = Cluster([cassandra_host], port=cassandra_port)
    session = cluster.connect(cassandra_keyspace)
    return session

cassandra_session = create_cassandra_session()

# Register ingest stream with Quine
def create_ingest_stream(ingest_stream_name):
    url = f"{quine_url}/ingest/{ingest_stream_name}"
    stream_definition = {
        "type": "KafkaIngest",
        "topics": [input_topic],
        "bootstrapServers": f"{redpanda_host}:{redpanda_port}",
        "groupId": "address-processor",
        "securityProtocol": "SASL_PLAINTEXT",
        "saslMechanism": "SCRAM-SHA-256",
        "saslUsername": "superuser",
        "saslPassword": "secretpassword"
    }

    response = requests.post(url, json=stream_definition)
    if response.status_code == 200:
        print(f"Kafka ingest stream '{ingest_stream_name}' registered successfully.")
    else:
        print(f"Failed to create ingest stream '{ingest_stream_name}': {response.status_code}")
        print(response.text)

# Send JSON payload to Quine
def ingest_to_quine(ingest_stream_name, payload):
    quine_ingest_url = f"{quine_url}/ingest/{ingest_stream_name}"
    try:
        response = requests.post(quine_ingest_url, json=payload)
        response.raise_for_status()
        print("Ingested to Quine:", payload)
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
            UUID(resolved_entity['resolved']),
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

# Send processed data to output Redpanda topic
def send_to_output_topic(processed_data):
    producer.produce(output_topic, processed_data.encode("utf-8"))
    producer.flush()
    print("Sent processed data to output topic.")

# Process incoming Kafka message
async def process_data(message, counter):
    try:
        raw_value = message.value().decode("utf-8")
        print("Incoming Kafka message:", raw_value)

        data = json.loads(raw_value)

        # Generate a unique ingest stream name for each message or batch
        ingest_stream_name = f"address-ingest-{uuid.uuid4()}"

        # Create the ingest stream with the unique name
        create_ingest_stream(ingest_stream_name)

        # Extract fields from the incoming data dynamically
        resolved_entity = {
            "addressee": data.get("addressee", "unknown"),
            "original": data.get("original", "unknown address"),
            "parts": {
                "city": data.get("city", "unknown"),
                "house": data.get("house", "unknown"),
                "houseNumber": data.get("houseNumber", "unknown"),
                "postcode": data.get("postcode", "unknown"),
                "road": data.get("road", "unknown"),
                "state": data.get("state", "unknown")
            },
            "resolved": str(uuid.uuid4())  # Generate a new resolved UUID each time
        }

        # 1. Insert into Cassandra
        insert_into_cassandra(resolved_entity)

        # 2. Ingest to Quine
        quine_payload = {
            "meta": {
                "isPositiveMatch": True
            },
            "data": {
                "resolved_entity": resolved_entity
            },
            "type": "KafkaIngest",
            "topics": [input_topic],
            "bootstrapServers": f"{redpanda_host}:{redpanda_port}"
        }
        ingest_to_quine(ingest_stream_name, quine_payload)

        # 3. Send to output Redpanda topic
        send_to_output_topic(json.dumps(quine_payload))

        # Increment counter
        counter += 1
        print(f"Processed message count: {counter}")

    except Exception as e:
        print(f"Error processing message: {e}")
    return counter

# Main loop to consume Kafka and ingest to Quine
def consume_messages():
    consumer.subscribe([input_topic])
    print("Listening for messages on:", input_topic)

    # Initialize message counter
    message_counter = 0

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        elif msg.error():
            print(f"Consumer error: {msg.error()}")
        else:
            message_counter = asyncio.run(process_data(msg, message_counter))

if __name__ == "__main__":
    threading.Thread(target=consume_messages).start()
