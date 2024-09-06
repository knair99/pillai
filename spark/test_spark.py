from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Create a Spark session
spark = (SparkSession.builder
         .appName("KafkaToKafka")
         .master("local[*]")  # Running locally, use all available cores
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2")
         .config("spark.driver.memory", "2g")  # Optionally adjust memory if needed
         .config("spark.executor.memory", "2g")
         .config("spark.sql.shuffle.partitions", "2")  # Set the number of partitions
         .config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())

# Set the Kafka broker address (use localhost because we're running everything locally)
kafka_brokers = "localhost:29092"

# Step 1: Read from Kafka topic 'lua_scripts'
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_brokers)
      .option("subscribe", "lua_scripts")
      .option("startingOffsets", "earliest")  # You can change this to 'latest' if needed
      .load())

# Step 2: Cast the key and value from Kafka as strings (assuming Lua scripts are in JSON format)
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Simulate malware detection processing
# Let's assume the result will be the same key with a new 'malware' field (mock detection logic)
processed_df = df.withColumn("malware_result", expr("IF(value LIKE '%malware%', 'detected', 'clean')"))

# Step 3: Write the result to another Kafka topic 'malware_results'
query = (processed_df
         .selectExpr("CAST(key AS STRING) AS key", "CAST(malware_result AS STRING) AS value")
         .writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", kafka_brokers)
         .option("topic", "malware_results")
         .option("checkpointLocation", "/tmp/spark_checkpoint")  # Set a checkpoint location
         .start())

# Wait for the termination of the stream
query.awaitTermination()
