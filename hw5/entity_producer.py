from confluent_kafka import Producer
import json

# Redpanda Producer Configuration
producer_conf = {
    "bootstrap.servers": "localhost:19092",
    "security.protocol": "SASL_PLAINTEXT", 
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "superuser", 
    "sasl.password": "secretpassword" 
}

producer = Producer(producer_conf)
input_topic = "address_input"

# Function to produce data to Redpanda
def produce_data(data):
    try:
        # Convert data to JSON string
        message = json.dumps(data)

        # Send the message to Redpanda (address_input topic)
        producer.produce(input_topic, message.encode('utf-8'))

        # Ensure the message is sent
        producer.flush()
        print("Message sent to Redpanda:", data)
    except Exception as e:
        print("Error producing message:", e)

# Read the real data from the NDJSON file
def read_and_produce(file_name):
    with open(file_name, "r") as f:
        for line in f:
            try:
                # Parse each line as a JSON object
                data = json.loads(line.strip())

                # Here, you can modify the data structure as needed before producing
                raw_data = {
                    "original": data.get("original"),
                    "addressee": data.get("addressee"),
                    "parts": data.get("parts")
                }

                # Produce the data to Redpanda
                produce_data(raw_data)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

if __name__ == "__main__":
    file_name = "public-record-addresses-2021.ndjson"
    read_and_produce(file_name)
