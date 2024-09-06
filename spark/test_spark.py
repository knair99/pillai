

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StringType, StructType, StructField

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
kafka_bootstrap_servers = "localhost:29092"
kafka_topic = "lua_scripts"
processed_topic = "processed_malware_results"

# Kafka schema
# kafka_schema = StructType([
#     #StructField("script", StringType(), True), # this could be wrong, since i am only applying the schema to the value
#     StructField("script", StringType(), True)
# ])


# Define the UDF for detecting malware in Lua scripts
def detect_malware(script):
    suspicious_keywords = ['while true do', 'os.execute', 'io.popen']
    for keyword in suspicious_keywords:
        if keyword in script.lower():
            return 'malicious'
    return 'safe'


# Initialize spark session
def init_spark_session():
    try:
        logger.info("pillai:spark: Starting Spark session...")
        spark = (SparkSession.builder
                 .appName("MalwareLuaScriptProcessing")
                 .master("spark://localhost:7077")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") # TODO: CHECK if this is needed
                 .getOrCreate())
        logger.info("pillai:spark: Spark session started.")
    except Exception as e:
        logger.error("pillai:spark: Error starting Spark session: %s", e)
        sys.exit(1)
    return spark


# Read from Kafka
def read_from_kafka(spark_session):
    try:
        logger.info("pillai:spark: Reading from Kafka topic: %s", kafka_topic)
        kafka_df = (spark_session.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                    .option("subscribe", kafka_topic)
                    .option("startingOffsets", "earliest")
                    .load())
        kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        logger.info("pillai:spark: Kafka DataFrame created. Schema: %s", kafka_df.schema.simpleString())
    except Exception as e:
        logger.error("pillai:spark: Error reading from Kafka: %s", e)
        sys.exit(1)
    return script_name, json_df


# Apply the UDF to detect malware
def apply_udf_to_malware(json_df):
    try:
        logger.info("pillai:spark: Applying malware detection UDF...")
        df_with_detection = json_df.selectExpr("data.script as script")
        df_with_detection = df_with_detection.withColumn("script", detect_malware_udf(col("script")))
        logger.info("pillai:spark: Malware detection UDF applied. Schema: %s", df_with_detection.schema.simpleString())
    except Exception as e:
        logger.error("pillai:spark: Error applying UDF: %s", e)
        sys.exit(1)
    return df_with_detection


# Write the results of processed malware to Kafka
def write_to_kafka(key, malware_processed_df):
    try:
        logger.info("pillai:spark: Starting query to process and publishing a message to Kafka...")

        def log_and_write_to_kafka(batch_df, batch_id):
            # Log the contents of each batch
            logger.info(f"pillai:spark: Processing batch {batch_id}")
            batch_df.show(truncate=False)

            # # Write the batch to Kafka
            batch_df.selectExpr("CAST(detection_result AS STRING) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", processed_topic) \
                .save()

        # Use foreachBatch to apply the log_and_write_to_kafka function on each batch
        query = malware_processed_df.writeStream \
            .foreachBatch(log_and_write_to_kafka) \
            .option("checkpointLocation", "/tmp/spark-checkpoint") \
            .start()

        query.awaitTermination()
        logger.info("pillai:spark: Query completed.")
    except Exception as e:
        logger.error("pillai:spark: Error in streaming query: %s", e)
        sys.exit(1)


# Register the UDF
#detect_malware_udf = udf(detect_malware, StringType())

# Initialize the Spark session
spark_session = init_spark_session()

# Read from Kafka
script_name, kafka_df = read_from_kafka(spark_session)

# Detect malware using simple rules
#df_with_detection = apply_udf_to_malware(kafka_df)

# Publish the results to Kafka
write_to_kafka(script_name, kafka_df)
