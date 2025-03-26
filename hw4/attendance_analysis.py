import redis
import json
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime, timedelta
from collections import defaultdict

# Configurations
REDIS_HOST = "localhost"
CASSANDRA_HOST = "localhost"
CASSANDRA_KEYSPACE = "attendance_keyspace"
CASSANDRA_TABLE = "attendance_records"
HYPERLOGLOG_KEY_PREFIX = "lecture_hyperloglog"

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

# Connect to Cassandra
def create_cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return session


# Estimate unique attendees
def estimate_unique_attendees(lecture_id):
    unique_attendees = redis_client.execute_command("PFCOUNT", f"{HYPERLOGLOG_KEY_PREFIX}:{lecture_id}")
    print(f"Estimated unique attendees for lecture '{lecture_id}': {unique_attendees}")
    return unique_attendees


# Identify habitual latecomers
def get_latecomers(session):
    query = f"""
    SELECT student_id, scheduled_lecture_time, actual_attendance_time 
    FROM {CASSANDRA_TABLE}
    WHERE status = 'present'
    ALLOW FILTERING
    """
    rows = session.execute(query)
    
    # Track number of late times per student
    latecomers = defaultdict(int) 

    for row in rows:
        actual_time = row.actual_attendance_time
        scheduled_time_str = f"{actual_time.strftime('%Y-%m-%d')} {row.scheduled_lecture_time}"
        scheduled_time = datetime.strptime(scheduled_time_str, '%Y-%m-%d %H:%M')

        # Calculate the time difference between actual attendance time and scheduled time
        time_difference = actual_time - scheduled_time

        if time_difference > timedelta(minutes=0):  # If the student is late
            latecomers[row.student_id] += 1  # Count the late occurrence for this student

    # Identify first-time latecomers and habitual latecomers
    first_time_latecomers = [(student_id, count) for student_id, count in latecomers.items() if count >= 1]
    habitual_latecomers = [(student_id, count) for student_id, count in latecomers.items() if count >= 2]

    print(f"Identified {len(first_time_latecomers)} latecomers.")
    print(f"Identified {len(habitual_latecomers)} habitual latecomers (late 2 or more times).")
    
    # Print the list of habitual latecomers
    '''
    for student_id, count in habitual_latecomers:
        print(f"  - Student {student_id} was late {count} times.")
    '''

    return habitual_latecomers


# Identify frequent absentees
def get_frequent_absentees(session):
    # First, get the count of absences for each student
    query = f"""
    SELECT student_id, COUNT(*) AS absentee_count FROM {CASSANDRA_TABLE}
    WHERE status = 'absent'
    GROUP BY student_id
    ALLOW FILTERING
    """
    rows = session.execute(query)

    # Filter out students with 1 or more absences
    absentees = [(row.student_id, row.absentee_count) for row in rows if row.absentee_count >= 1]
    print(f"Found {len(absentees)} absentees.")

    # Filter out students with 2 or more absences
    habitual_absentees = [(row.student_id, row.absentee_count) for row in rows if row.absentee_count >= 2]
    print(f"Found {len(habitual_absentees)} frequent absentees (more than 2 absences).")
   
   # Print the list of frequent absentees
    '''
    for absentee in absentees:
        print(f"  - Student {absentee[0]} has missed {absentee[1]} lectures.")
    '''
    return absentees


# Find Students with Perfect Attendance with no absences or late
def get_perfect_attendance(session):
    query_total = f"""
    SELECT student_id, actual_attendance_time, scheduled_lecture_time
    FROM {CASSANDRA_TABLE}
    WHERE status = 'present'
    ALLOW FILTERING
    """
    rows_total = session.execute(query_total)
    total_attendees = {}

    for row in rows_total:
        if row.student_id not in total_attendees:
            total_attendees[row.student_id] = {
                'total': 0,
                'on_time': 0
            }
        total_attendees[row.student_id]['total'] += 1
   
        actual_time = row.actual_attendance_time.time() 
        scheduled_time = datetime.strptime(row.scheduled_lecture_time, "%H:%M").time()

        # Check if the student was on time (compare time part only)
        if actual_time <= scheduled_time:
            total_attendees[row.student_id]['on_time'] += 1

    # Identify students with perfect attendance
    perfect_attendees = [
        student_id for student_id, attendance in total_attendees.items()
        if attendance['total'] == attendance['on_time']
    ]
    print(f"Found {len(perfect_attendees)} students with perfect attendance.")

    # Print the list of students with perfect attendance
    '''
    for student in perfect_attendees:
        print(f"  - Student {student} has attended all lectures on time.")
    '''
    return perfect_attendees


def get_top_attendees(session):
    query = f"""
    SELECT student_id 
    FROM {CASSANDRA_TABLE}
    WHERE status = 'present'
    ALLOW FILTERING
    """
    rows = session.execute(query)
    
    # Count attendances per student
    attendance_count = defaultdict(int)
    for row in rows:
        attendance_count[row.student_id] += 1
    
    # Sort by attendance count in descending order
    top_attendees = sorted(attendance_count.items(), key=lambda x: x[1], reverse=True)[:10]
    
    print(f"Top 10 students with the highest attendance.")
    for student in top_attendees:
        print(f"  - Student {student[0]} attended {student[1]} lectures.")
    
    return top_attendees


def main():
    session = create_cassandra_session()

    latecomers = get_latecomers(session)
    absentees = get_frequent_absentees(session)
    perfect_attendance = get_perfect_attendance(session)
    top_attendees = get_top_attendees(session)
    lecture_id = "2025-02-15-12:00-Art"
    unique_attendees = estimate_unique_attendees(lecture_id)

if __name__ == "__main__":
    main()
