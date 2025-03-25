import pulsar
import redis
import json
import asyncio
from aiocassandra import aiosession
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime

# Configurations
PULSAR_URL = "pulsar://localhost:6650"
PULSAR_TOPIC = "attendance"
REDIS_HOST = "localhost"
CASSANDRA_HOST = "localhost"
CASSANDRA_KEYSPACE = "attendance_keyspace"
CASSANDRA_TABLE = "attendance_records"

# Redis Keys
BLOOM_FILTER_KEY = "student_bloom_filter"
HYPERLOGLOG_KEY_PREFIX = "lecture_hyperloglog"

# Set up Redis connection
redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

# Set up Pulsar client and consumer
client = pulsar.Client(PULSAR_URL)
consumer = client.subscribe(PULSAR_TOPIC, subscription_name="attendance-subscription")


# Create Cassandra session asynchronously
async def create_cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    # Create keyspace if not exists
    create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """
    session.execute(create_keyspace_query)
    session.set_keyspace(CASSANDRA_KEYSPACE)

    # Create table if not exists
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
        student_id INT,
        lecture_id TEXT,
        timestamp TIMESTAMP,
        scheduled_lecture_time TEXT,
        actual_attendance_time TIMESTAMP,
        PRIMARY KEY (student_id, lecture_id)
    );
    """
    session.execute(create_table_query)

    # Return async session wrapped with aiosession
    aio_session = aiosession(session)
    return aio_session


# Initialize Bloom Filter
def init_bloom_filter():
    if not redis_client.exists(BLOOM_FILTER_KEY):
        redis_client.execute_command("BF.RESERVE", BLOOM_FILTER_KEY, 0.01, 1000000)
        print("Bloom Filter initialized.")
    else:
        print("Bloom Filter already exists.")


# Initialize HyperLogLog for a given lecture ID
def init_hyperloglog(lecture_id):
    print(f"Adding student to HyperLogLog for lecture {lecture_id}...")
    redis_client.execute_command("PFADD", f"{HYPERLOGLOG_KEY_PREFIX}:{lecture_id}")
    print(f"Student added to HyperLogLog for lecture {lecture_id}.")


# Insert attendance record into Cassandra
async def insert_attendance_record(session, student_id, lecture_id, scheduled_time, actual_attendance_time):
    query = f"""
    INSERT INTO {CASSANDRA_TABLE} (student_id, lecture_id, timestamp, scheduled_lecture_time, actual_attendance_time)
    VALUES (%s, %s, %s, %s, %s)
    """
    print(f"Inserting attendance record into Cassandra: {student_id}, {lecture_id}, {actual_attendance_time}")
    statement = SimpleStatement(query)
    session.execute(statement, (student_id, lecture_id, actual_attendance_time, scheduled_time, actual_attendance_time))
    print(f"Attendance record for student {student_id} in lecture {lecture_id} inserted into Cassandra.")


# Process incoming Pulsar message
async def process_message(session, msg):
    message_data = json.loads(msg.data())
    student_id = message_data['student_id']
    lecture_id = message_data['lecture_id']
    scheduled_lecture_time = message_data['scheduled_lecture_time']
    actual_attendance_time = datetime.fromisoformat(message_data['actual_attendance_time'])

    print(f"Received message: {message_data}")

    # Check if the student ID exists in the Bloom Filter
    if redis_client.execute_command("BF.EXISTS", BLOOM_FILTER_KEY, student_id):
        print(f"Student {student_id} is valid. Processing attendance.")

        # Add the student to the HyperLogLog
        init_hyperloglog(lecture_id)

        # Insert attendance record into Cassandra
        await insert_attendance_record(session, student_id, lecture_id, scheduled_lecture_time, actual_attendance_time)
    else:
        print(f"Student {student_id} is not in Bloom Filter. Adding to Bloom Filter.")

        # Add student to the Bloom Filter
        try:
            redis_client.execute_command("BF.ADD", BLOOM_FILTER_KEY, student_id)
            print(f"Student {student_id} added to Bloom Filter.")
        except redis.exceptions.ResponseError:
            print(f"Student {student_id} already exists in Bloom Filter.")


async def main():
    init_bloom_filter()
    session = await create_cassandra_session()

    while True:
        msg = await asyncio.to_thread(consumer.receive)
        try:
            await process_message(session, msg)
            consumer.acknowledge(msg)
        except Exception as e:
            print(f"Error processing message: {e}")
            consumer.negative_acknowledge(msg)

if __name__ == "__main__":
    asyncio.run(main())
