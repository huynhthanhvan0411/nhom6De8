import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, min, max, avg,when,count, sum

# Create a Spark session
spark = SparkSession.builder.appName("bigdata").getOrCreate()
# Load the CSV data with the defined schema
data = spark.read.format('csv') \
    .option('header', 'true') \
    .option('escape', '\"') \
    .load('D:/WORK_UTT_F/nam_4/ki_1/part_1/big_data_demo/de8Nhom6/E1.csv')


data = data.withColumn("FTHG", data["FTHG"].cast(IntegerType()))
data = data.withColumn("FTAG", data["FTAG"].cast(IntegerType()))
data = data.withColumn("HS", data["HS"].cast(IntegerType()))
data = data.withColumn("AS", data["AS"].cast(IntegerType()))


# data.printSchema()

# tìm số đột trong mua giải
# đổi tên cột 
cau3_3 = data.withColumnRenamed("HomeTeam","ListTeam")
# lựa chọn cột rồi loại bỏ trùng lặp 
cau3 = cau3_3.select("ListTeam").distinct()
# cau3.show(25)


# . Tìm số trận có kết quả hoà 
# lọc cột có hòa D
# cau4 = data.filter(data["FTR"] == "D")
# cau4_count = cau4.count()
# print("Câu 3.4: Số trận có kết quả hòa")
# print("Số trận có kết quả hòa: " + str(cau4_count))
# print("==========================================================")
# print("Câu 3.4: Các trận đấu có kết quả hòa và các đội tham gia:")
# cau4.select("HomeTeam", "AwayTeam", "FTHG").show(n=cau4.count(), truncate=False)

# tổng số bàn thắng đội đá trên sân nhà ghi được
# bàn thắng nằm cột fthg, tính tổng các lần thắng cột đó đội
# gom theo tea rồi tins tổng của nhom đó và đổi tên 
# cau35 = data.groupBy("HomeTeam").agg({"FTHG":"sum"}).withColumnRenamed("sum(FTHG)", "Totall")
# # tính tổng hàng cuối
# total_goals = cau35.selectExpr("sum(Totall) as Tota").first().Tota
# print("Tổng số bàn thắng các đội đá sân nhà ghi được: " + str(total_goals))
# cau35.show()

# Tìm những trận có tổng số bàn thắng > 3
# ta có cột hthg, htag là đại diện tổng số bàn thắng cuối trận của chủ nhà và khách, cộng lại lớn hơn 3 thì ok 
# # ta tạo 1 cột mới, cột đó là tinh tổng
# cau36= data.withColumn("TongGoal", col("FTHG") +col("FTAG")).filter(col("TongGoal") >3)

# cau36.select("HomeTeam", "AwayTeam", "TongGoal").show()

# 3.7. Tìm những trận của Burnley được thi đấu trên sân nhà và có số bàn thắng >=3 (Tính cả của đội khách)
# tong ban thang
# total_goals= data.withColumn("Totals", col("FTHG") + col("FTAG"))
# cau37 = total_goals.filter((col("HomeTeam")=="Burnley")& (col("Totals") >= 3))
# cau7 = cau37.select("HomeTeam", "AwayTeam","Totals").show()

# 3.8. Tìm những trận mà Reading thua (Không được sử dụng cột FTR)
# phân là m 2 lần, khi là chủ nhà thua và khi khách thua, dùng cột ghi điểm cuối trận thấp hơn thi thua 
# 1. khi là chủ nhà
# homeTeam = data.filter((col("HomeTeam")=="Reading") &(col("FTHG") < col("FTAG"))).select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG")
# # 2. khi là đọio khách
# awayTeam = data.filter((col("AwayTeam")=="Reading") &(col("FTHG") > col("FTAG"))).select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG")

# cau8= awayTeam.union(homeTeam)
# cau8.show()

# 3.9. Xoay giá trị trong cột FTR thành các cột, với mỗi cột chứa số lượng FTR tương ứng. nhóm theo HomeTeam
# tạo 4 cột hometeam xem a d h ứng xem tổng số lần

cau9_data= data.groupBy("HomeTeam", "FTR").agg(count("FTR").alias("Countss"))
cau9 = cau9_data.groupBy("HomeTeam").pivot("FTR").agg(sum("Countss"))
cau9.show()
# 3.10. Tạo một cột mới với tên cột tuỳ chọn: Nếu tổng số bàn thắng 2 đội ghi được trong trận  <2 thì điền “well” , nếu số bàn thắng  2 < x < 4 thì điền “very good”, nếu số bàn thắng >= 4 thì điền “amazing”
# cau10 = data.withColumn("Toltalls", col("FTHG") + col("FTAG"))
# # cau10= cau10.withColumn("Status",
# #          when(cau10["Toltalls"]  <2, "well")
# #         .when(cau10["Toltalls"] >=2 & cau10["Toltalls"] <4, "very good" )
# #         .otherwise("amazing"))
# cau10 = cau10.withColumn(
#         "Status",
#         when(cau10["Toltalls"] < 2, "well")
#         .when((cau10["Toltalls"] >= 2) & (cau10["Toltalls"] < 4), "very good")
#         .otherwise("amazing")
#     )

# print("Câu 3.10")
# cau10.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "Toltalls", "Status").show(n=cau10.count(), truncate=False)
