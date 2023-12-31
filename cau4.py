from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType,IntegerType


spark = SparkSession.builder.appName("cau4").getOrCreate()

schema = StructType([
    StructField("Div", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("HomeTeam", StringType(), True),
    StructField("AwayTeam", StringType(), True),
    StructField("FTHG", IntegerType(), True),
    StructField("FTAG", IntegerType(), True),
    StructField("FTR", StringType(), True),
    StructField("HTHG", IntegerType(), True),
    StructField("HTAG", IntegerType(), True),
    StructField("HTR", StringType(), True),
    StructField("HS", IntegerType(), True),
    StructField("AS", IntegerType(), True),
    StructField("HST", StringType(), True),
    StructField("AST", StringType(), True),
    StructField("HF", StringType(), True),
    StructField("AF", StringType(), True),
    StructField("HC", StringType(), True),
    StructField("AC", StringType(), True),
    StructField("HY", StringType(), True),
    StructField("AY", StringType(), True),
    StructField("HR", StringType(), True),
    StructField("AR", StringType(), True),
    StructField("B365H", StringType(), True),
    StructField("B365D", StringType(), True),
    StructField("B365A", StringType(), True),
])

common_schema = StructType([
        StructField("Div", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("HomeTeam", StringType(), True),
        StructField("AwayTeam", StringType(), True),
        StructField("FTHG", IntegerType(), True),
        StructField("FTAG", IntegerType(), True),
        StructField("FTR", StringType(), True),
        StructField("HTHG", IntegerType(), True),
        StructField("HTAG", IntegerType(), True),
        StructField("HTR", StringType(), True),
        StructField("HS", IntegerType(), True),
        StructField("AS", IntegerType(), True),
        StructField("HST", StringType(), True),
        StructField("AST", StringType(), True),
        StructField("HF", StringType(), True),
        StructField("AF", StringType(), True),
        StructField("HC", StringType(), True),
        StructField("AC", StringType(), True),
        StructField("HY", StringType(), True),
        StructField("AY", StringType(), True),
        StructField("HR", StringType(), True),
        StructField("AR", StringType(), True),
        StructField("B365H", StringType(), True),
        StructField("B365D", StringType(), True),
        StructField("B365A", StringType(), True),
    ])


# Đọc dữ liệu sử dụng readStream
dataStream = spark.readStream \
    .format("csv") \
    .option("header", "true").schema(schema) \
    .load("stream/")

dataStream1 = spark.readStream \
    .format("csv") \
    .option("header", "true").schema(common_schema) \
    .load("stream1/")

dataS = dataStream.select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG")
dataS1 = dataStream1.select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG")
dataUnion = dataS.union(dataS1)
goal_counts = dataUnion.filter((col("FTHG") == col("HTHG")) & (col("FTAG") == col("HTAG")))

result = goal_counts.withColumn("TotalGoals", col("FTHG") + col("FTAG"))
# Ghi ra số bàn thắng và sử dụng checkpoint
query = result.writeStream \
    .outputMode("append") \
    .option("truncate", False) \
    .format("console") \
    .start()
# Chờ cho đến khi người dùng dừng ứng dụng
query.awaitTermination()
