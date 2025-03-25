import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('attendance', subscription_name='my-sub')

while True:
    msg = consumer.receive()
    print("Received message: '%s'" % msg.data())
    consumer.acknowledge(msg)

client.close()