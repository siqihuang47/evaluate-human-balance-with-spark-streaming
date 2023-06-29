from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

# Using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
spark = SparkSession.builder.appName("StediKafkaEvents").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

kafkaEventDF = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "stedi-events")\
    .option("startingOffsets", "earliest")\
    .load()

# Cast the value column in the streaming dataframe as a STRING 
kafkaEventDF = kafkaEventDF.selectExpr("CAST(value AS string) value")

# Parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
kafkaEventSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", DoubleType()),
        StructField("riskDate", DateType())
    ]
)

kafkaEventDF = kafkaEventDF.withColumn("value", from_json("value", kafkaEventSchema))\
    .select("value.customer", "value.score", "value.riskDate")\
    .createOrReplaceTempView("CustomerRisk")

# Execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql(
    "SELECT customer, score FROM CustomerRisk")

# Sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct
customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()