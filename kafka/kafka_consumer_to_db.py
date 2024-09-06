import logging
import os
import sys
from confluent_kafka import Consumer, KafkaException, KafkaError

# Add the parent directory of 'kafka/' and 'db/' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now import the functions from db_ops
from db.db_ops import connect_to_db, write_to_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration for the Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'db_writer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start reading at the earliest offset
}

# Kafka consumer loop
def consume_loop(consumer, connection):
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for new messages

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.info(
                        f"pillai:kafka:consumer:db writer: Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                logger.info(f"pillai:kafka:consumer:db writer: Received message: {msg.value()}")
                write_to_db(connection, msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    # Initialize Kafka consumer
    kafka_consumer = Consumer(conf)
    kafka_consumer.subscribe(['malware_results'])

    # Connect to the database
    db_connection = connect_to_db()
    if db_connection:
        try:
            consume_loop(kafka_consumer, db_connection)
        finally:
            db_connection.close()
