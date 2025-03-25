import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('attendance')

for i in range(10):
    producer.send(('sending test messages-%d' % i).encode('utf-8'))

client.close()