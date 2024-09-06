import logging
import psycopg2
import json
from confluent_kafka import Consumer, KafkaException, KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration for the Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'db_writer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start reading at the earliest offset
}

# Database configuration
db_host = "localhost"
db_port = "26257"
db_name = "defaultdb"
db_user = "root"
db_password = ""


# Connect to the database
def connect_to_db():
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        logger.info("pillai:kafka:consumer:db writer: Connected to the database")
        return connection
    except Exception as e:
        logger.error(f"pillai:kafka:consumer:db writer: Error connecting to the database: {e}")
        return None


# Write message to the database
def write_to_db(connection, msg):
    try:
        value = msg.value()
        logger.info(f"pillai:kafka:consumer:db writer: Message value type: {type(value)}")

        if value is None or value == b'':
            logger.warning("pillai:kafka:consumer:db writer: Empty message received, skipping.")
            return

        row = json.loads(value)  # Decode and parse JSON
        logger.info(f"pillai:kafka:consumer:db writer: Decoded message: {row}")

        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO malware_results (script, detection_result) VALUES (%s, %s)",
            (row['script'], row['detection_result'])
        )
        connection.commit()
        cursor.close()
        logger.info("pillai:kafka:consumer:db writer: Message written to the database")
    except Exception as e:
        logger.error(f"pillai:kafka:consumer:db writer: Error writing to DB: {e}")


# Kafka consumer loop
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
                    logger.info(f"pillai:kafka:consumer:db writer: Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
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
