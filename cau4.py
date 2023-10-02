import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define schema for DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
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

# Đọc dữ liệu từ file CSV: Trước tiên, bạn cần đọc dữ liệu từ tệp CSV của giải đấu La Liga từ năm 2018 đến 2021.

# Xác định các trận đấu được kết thúc trong một hiệp: 
# Bạn cần kiểm tra xem trận đấu nào không có bàn thắng ở hiệp thứ 2. 
# Điều này có nghĩa là (HTHG + FTAG) == FTHG và (HTAG + HG) == FTAG.

# Ghi ra số bàn thắng ghi được trong trận đấu đó: Sau khi xác định các trận đấu phù hợp, bạn có thể tính tổng số bàn thắng ghi được trong trận đấu bằng cách thêm FTHG và FTAG lại với nhau.

# Create a SparkSession with userClassPathFirst set to true
spark = SparkSession.builder \
    .appName("Nhom6Cau4") \
    .getOrCreate()


# Read the CSV file
data = spark.readStream \
    .schema(schema) \
    .option('header', 'true') \
    .format("csv") \
    .option('path', 'D:/WORK_UTT_F/nam_4/ki_1/part_1/big_data_demo/de8Nhom6/stream/') \
    .load()

data = data.withColumn("FTHG", data["FTHG"].cast(IntegerType()))
data = data.withColumn("FTAG", data["FTAG"].cast(IntegerType()))
data = data.withColumn("HS", data["HS"].cast(IntegerType()))
data = data.withColumn("AS", data["AS"].cast(IntegerType()))


dfs = data.select("Date","HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG")
goal_counts = dfs.filter((col("FTHG") == col("HTHG")) & (col("FTAG") == col("HTAG")))

result = goal_counts.withColumn("TotalGoals", col("FTHG") + col("FTAG"))

# Ghi ra số bàn thắng
query = result.writeStream \
    .outputMode("append") \
    .option("truncate", False) \
    .format("console") \
    .option('truncate', 'false') \
    .option('numRows', 1000)\
    .start()


# Chờ cho đến khi người dùng dừng ứng dụng
query.awaitTermination()
