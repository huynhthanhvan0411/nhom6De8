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

# Sắp xếp tăng dần theo cột 'age'
sorted_df = df.orderBy("age")

# Sắp xếp giảm dần theo cột 'age'
# sorted_df = df.orderBy("age", ascending=False)
print("Tổng của cột 'age' cho những người có tuổi bằng 25 là:", sum_age)
print("Dữ liệu sau khi sắp xếp tăng dần theo cột 'age':")
sorted_df.show()
# Đếm số dòng có giá trị "very good" và "amazing" trong cột "Status"
# count_very_good = cau8.filter(cau8["Status"] == "very good").count()
# count_amazing = cau8.filter(cau8["Status"] == "amazing").count()
