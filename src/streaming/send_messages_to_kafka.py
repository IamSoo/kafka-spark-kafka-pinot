import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
from pyspark.sql import SparkSession
import time

KAKFA_BOOTSTRAP_SERVICES = "kafka:9092"
KAFKA_TOPIC = "cars_sales"
FILE_PATH = "/data/json/"


SCHEMA = StructType([
    StructField("Manufacturer", StringType()),
    StructField("Model", StringType()),
    StructField("Sales_in_thousands", StringType()),
    StructField("__year_resale_value", StringType()),
    StructField("Vehicle_type", StringType()),
    StructField("Price_in_thousands", StringType()),
    StructField("Engine_size", StringType()),
    StructField("Horsepower", StringType()),
    StructField("Wheelbase", StringType()),
    StructField("Width", StringType()),
    StructField("Length", StringType()),
    StructField("Curb_weight", StringType()),
    StructField("Fuel_capacity", StringType()),
    StructField("Fuel_efficiency", StringType()),
    StructField("Fuel_efficiencyFuel_efficiency", StringType()),
    StructField("Latest_Launch", StringType()),
    StructField("Power_perf_factor", StringType())
])

spark = SparkSession.builder.appName("Write car data to topic").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_car_sales = spark.read.format("json")\
    .schema(SCHEMA)\
    .load(FILE_PATH)\
    .withColumn("value", F.to_json(F.struct(F.col("*"))))\
    .withColumn("key", F.lit("key")) \
    .withColumn("value", F.encode(F.col("value"), "iso-8859-1").cast("binary"))\
    .withColumn("key", F.encode(F.col("key"), "iso-8859-1").cast("binary"))\
    .select("key", "value")\
    .limit(10)\

df_car_sales.printSchema()
#
# query = df_car_sales.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .format("console") \
#     .option("checkpointLocation", "path/to/HDFS/dir") \
#     .start()\
#     .awaitTermination()

# df_car_sales.writeStream.format("console").start().awaitTermination()

for row in df_car_sales.collect():
    each_row = spark.createDataFrame([row.asDict()])
    each_row.write\
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAKFA_BOOTSTRAP_SERVICES) \
        .option("topic", KAFKA_TOPIC)\
        .option("checkpointLocation", "/tmp/checkpoint")\
        .save()
    print(f"Row written to topic {KAFKA_TOPIC}")
    time.sleep(2.5)
# df_car_sales.select("key","value")\
#     .writeStream\
#     .format("kafka")\
#     .option("kafka.bootstrap.servers", KAKFA_BOOTSTRAP_SERVICES)\
#     .option("topic", KAFKA_TOPIC)\
#     .option("checkpointLocation", "/tmp/checkpoint")\
#     .start()\
#     .awaitTermination()