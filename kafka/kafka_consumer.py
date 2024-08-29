from confluent_kafka import Consumer, KafkaException, KafkaError

# Configuration for the Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:29092',  # Update this to your Kafka server address
    'group.id': 'game_consumer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start reading at the earliest offset
}

# Create a Consumer instance
kafka_consumer = Consumer(conf)

# Subscribe to the topic
kafka_consumer.subscribe(['test_topic'])


# Function to handle incoming messages
def consume_loop(consumer, topics):
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'\n pillai:kafka:consumer: {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                print(f'\n pillai:kafka:consumer: Received message: {msg.value().decode("utf-8")} from topic {msg.topic()}')

    except KeyboardInterrupt:
        print(f'\n pillai:kafka:consumer: Exiting - interrupted by user')
        exit()
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


# Start consuming messages
consume_loop(kafka_consumer, ['test_topic'])
