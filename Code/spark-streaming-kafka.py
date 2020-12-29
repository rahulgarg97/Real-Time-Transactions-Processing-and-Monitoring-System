from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# from pyspark.sql import functions as f
# from pyspark.sql.avro.functions import from_avro, to_avro

KAFKA_TOPIC_NAME_CONS = "transactionrequest"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "transactiondetail"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars", "spark-sql-kafka-0-10_2.11-2.4.0.jar,kafka-clients-1.1.0.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from testtopic
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()

    transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the transaction_detail data
    transaction_detail_schema = StructType() \
        .add("transaction_id", StringType()) \
        .add("user_id", StringType()) \
        .add("card_number", StringType()) \
        .add("amount", StringType()) \
        .add("description", StringType()) \
        .add("transaction_type", StringType()) \
        .add("vendor", StringType()) \

    transaction_detail_df2 = transaction_detail_df1\
        .select(from_json(col("value"), transaction_detail_schema).alias("transaction_detail"), "timestamp")

    transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

    transaction_detail_df3 = transaction_detail_df3.withColumn("transaction_id",transaction_detail_df3["transaction_id"].cast('int')).withColumn("user_id",transaction_detail_df3["user_id"].cast('int')).withColumn("card_number",transaction_detail_df3["card_number"].cast('long')).withColumn("amount",transaction_detail_df3["amount"].cast('float')).withColumn("status", lit("None"))
    transaction_detail_df3 = transaction_detail_df3.withColumn("status",when(transaction_detail_df3.amount <= 150, "APPROVED").otherwise('DENIED'))

    transaction_detail_df3 = transaction_detail_df3.withColumn("key", lit(100))\
                                                .withColumn("value", concat(\
                                                col("transaction_id"), lit(" "), \
                                                col("user_id"), lit(" "), \
                                                col("card_number"), lit(" "), \
                                                col("amount"), lit(" "), \
                                                col("description").cast("string"), lit(" "), \
                                                col("transaction_type").cast("string"), lit(" "), \
                                                col("vendor").cast("string"), lit(" "), \
                                                col("status").cast("string")))

    # Write final result into console for debugging purpose
    trans_detail_write_stream = transaction_detail_df3 \
        .writeStream \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    trans_detail_write_stream_1 = transaction_detail_df3 \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("checkpointLocation", "file:///Users/rahulgarg97/Pictures/sparkwithkafka/py_checkpoint") \
        .start()

    trans_detail_write_stream.awaitTermination()
    # trans_detail_write_stream_denied.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed.")