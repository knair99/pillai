import logging

import psycopg2

# Database configuration
db_host = "localhost"
db_port = "26257"
db_name = "defaultdb"
db_user = "root"
db_password = ""

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

        # row = json.loads(value)  # Decode and parse JSON
        # logger.info(f"pillai:kafka:consumer:db writer: Decoded message: {row}")

        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO malware_detection (script, result) VALUES (%s, %s)",
            (msg.key().decode('unicode_escape'), msg.value().decode('unicode_escape'))
        )
        connection.commit()
        cursor.close()
        logger.info("pillai:kafka:consumer:db writer: Message written to the database")
    except Exception as e:
        logger.error(f"pillai:kafka:consumer:db writer: Error writing to DB: {e}")
