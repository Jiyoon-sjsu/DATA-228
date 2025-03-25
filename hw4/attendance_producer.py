import random
import time
from pulsar import Client
from faker import Faker
import json
from datetime import datetime
 
client = Client("pulsar://localhost:6650")
producer = client.create_producer("persistent://public/default/attendance")
faker = Faker()

# Generate a random lecture ID
def generate_lecture_id():
    course_name = faker.random_element(elements=("Math", "Science", "History", "Literature", "Art"))
    lecture_date = faker.date_this_year().strftime('%Y-%m-%d')
    lecture_time = random.choice(["09:00", "12:00", "15:00", "18:00"]) 
    return f"{lecture_date}-{lecture_time}-{course_name}"

dummy_data = {
    "student_id": 12345,
    "lecture_id": "2023-09-01-10:00-Math",
    "timestamp": "2023-09-01T10:00:00"
}

'''
# Send attendance messages
for _ in range(3):
    message = json.dumps(dummy_data).encode()
    producer.send(message)
    print(f"Sent attendance message 3 times for test: {dummy_data}")
    time.sleep(1)  # Adjust delay as needed
'''

while True:
    # Generate a random student ID and lecture ID
    student_id = faker.unique.random_int(min=10000, max=99999)
    lecture_id = generate_lecture_id()

    message_data = {
        "student_id": student_id,
        "lecture_id": lecture_id,
        "timestamp": datetime.now().isoformat() 
    }

    message = json.dumps(message_data).encode() 
    producer.send(message)

    print(f"Sent attendance message: {message_data}")

    # Simulate a small delay between swipes
    time.sleep(random.uniform(1, 2))  # Adjust delay as needed
    
client.close()