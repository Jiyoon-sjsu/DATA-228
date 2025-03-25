import random
import time
from pulsar import Client
from faker import Faker
import json
from datetime import datetime, timedelta

client = Client("pulsar://localhost:6650")
producer = client.create_producer("persistent://public/default/attendance")
faker = Faker()

# Generate a random lecture ID
def generate_lecture_id():
    course_name = faker.random_element(elements=("Math", "Science", "History", "Literature", "Art"))
    lecture_date = faker.date_this_year(before_today=True, after_today=False).strftime('%Y-%m-%d')
    lecture_time = generate_lecture_time()
    return f"{lecture_date}-{lecture_time}-{course_name}"

# Generate a random lecture time 
def generate_lecture_time():
    hour = random.choice([9, 12, 15, 18])  
    minute = random.choice([0, 15, 30, 45]) 
    return f"{hour:02}:{minute:02}"

# Simulate a random student being late (up to 30 minutes)
def simulate_lateness(lecture_time_str):
    try:
        lecture_time = datetime.strptime(lecture_time_str, "%H:%M")
        late_minutes = random.randint(0, 30)  
        late_time = lecture_time + timedelta(minutes=late_minutes)
        return late_time
    except ValueError:
        print(f"Error with time format: {lecture_time_str}")
        raise

# Handle the attendance time parsing
def handle_attendance_time(lecture_date_str, scheduled_lecture_time):
    try:
        return datetime.strptime(f"{lecture_date_str} {scheduled_lecture_time}", "%Y-%m-%d %H:%M")
    except ValueError as e:
        print(f"Error with datetime format: {scheduled_lecture_time}, error: {e}")
        raise

# Main loop to simulate attendance data generation
while True:
    student_id = faker.unique.random_int(min=10000, max=99999)  
    lecture_id = generate_lecture_id()
    try:
        lecture_date_str, scheduled_lecture_time, course_name = lecture_id.rsplit('-', 2)
        
    except ValueError as e:
        print(f"Error unpacking lecture_id '{lecture_id}': {e}")
        continue 

    if random.random() < 0.1:  # 10% chance of being late
        try:
            actual_attendance_time = simulate_lateness(scheduled_lecture_time)
        except ValueError:
            continue  
    else:
        try:
            actual_attendance_time = handle_attendance_time(lecture_date_str, scheduled_lecture_time)
        except ValueError:
            continue 

    # Create message with the student ID, lecture ID, and actual attendance timestamp
    message_data = {
        "student_id": student_id,
        "lecture_id": lecture_id,
        "scheduled_lecture_time": scheduled_lecture_time, 
        "actual_attendance_time": actual_attendance_time.isoformat(), 
    }

    message = json.dumps(message_data).encode() 
    producer.send(message)

    print(f"Sent attendance message: {message_data}")

    # Simulate a small delay between swipes (adjust as needed)
    time.sleep(random.uniform(0.1, 0.5))  # Simulate variable delays in student swipes

client.close()
