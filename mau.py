from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# Tạo DataFrame mẫu
data = [("Alice", 25),
        ("Bob", 30),
        ("Charlie", 25),
        ("David", 28)]

columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

# Tính tổng của cột 'age' cho những người có tuổi bằng 25
sum_age = df.filter(df.age == 25).agg({"age": "sum"}).collect()[0][0]

print("Tổng của cột 'age' cho những người có tuổi bằng 25 là:", sum_age)
