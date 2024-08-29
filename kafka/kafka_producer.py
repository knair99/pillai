from confluent_kafka import Producer

# Configuration for the Kafka producer
conf = {
    'bootstrap.servers': 'localhost:29092',
}

# Create a producer instance
producer = Producer(conf)


# Delivery callback for producer delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'\n pillai:kafka:producer: Message delivery failed: {err}')
    else:
        print(f'\n pillai:kafka:producer: Message delivered to {msg.topic()} [{msg.partition()}]')


# Produce a message
producer.produce('test_topic', key='key', value='Hello, Kafka!', callback=delivery_report)

# Wait for any outstanding messages to be delivered
producer.flush()
