import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, min, max, avg,when,count, sum

# tạo spark context
spark = SparkSession.builder.appName("bigdata").getOrCreate()

# đọc dữ liệu từ văn bản vào rdd 
data = spark.read.format('csv') \
    .option('header', 'true') \
    .option('escape', '\"') \
    .load('D:/WORK_UTT_F/nam_4/ki_1/part_1/big_data_demo/de8Nhom6/E1.csv')

data = data.withColumn("FTHG", data["FTHG"].cast(IntegerType()))
data = data.withColumn("FTAG", data["FTAG"].cast(IntegerType()))
data = data.withColumn("HS", data["HS"].cast(IntegerType()))
data = data.withColumn("AS", data["AS"].cast(IntegerType()))
# data.printSchema()

# 3.3. Có bao nhiêu đội đá trong mùa giải, liệt kê các đội
def cau1():
    cau1 = data.withColumnRenamed("HomeTeam", "List_Team").select("List_Team").distinct()
    print("Câu 3.3: Có " + str(cau1.count()) +" đội trong mùa giải")
    print("Danh sách các đội:")
    cau1.show(n=cau1.count(), truncate=False)
# 3.4. Tìm số trận có kết quả hoà
# (FTR= Full time result: H = homewin, D = Draw, A= Awaywin)
def cau2():
    cau2 = data.where(data["FTR"] == "D")
    print("Câu 3.4: Số trận có kết quả hòa")
    print("Số trận có kết quả hòa: " + str(cau2.count()))
    print("==========================================================")
    print("Câu 3.4: Các trận đấu có kết quả hòa và các đội tham gia:")
    cau2.select("HomeTeam", "AwayTeam").show(n=cau2.count(), truncate=False)
# 3.5. Tìm tổng số bàn thắng các đội đá trên sân nhà ghi được 
# (FTHG = Full Time hometeam goal – FTAG: Full Time AwayGoal)
def cau3():
    print("Câu 3.5: Tìm tổng số bàn thắng các đội ghi được trên sân nhà")
    cau3 = data.groupBy("HomeTeam").agg({"FTHG": "sum"}).withColumnRenamed("sum(FTHG)", "Total")
    cau3.show(n=cau3.count(), truncate=False)
    # tính toorng full số bàn thắng ở cột total 
    total_goals = cau3.selectExpr("sum(Total) as Total").first().Total
    print("Tổng số bàn thắng các đội đá sân nhà ghi được: " + str(total_goals))
# 3.6. Tìm những trận có tổng số bàn thắng > 3
def cau4():
    print("Câu 3.6:")
    cau4 = data.withColumn("TotalGoals", col("FTHG") + col("FTAG")).filter(col("TotalGoals") > 3)
    # tính tổng 
    print("Số trận có tổng số bàn thắng > 3: " + str(cau4.count()))
    # in hàng
    print("Câu 3.6: Những trận có tổng số bàn thắng > 3")
    cau4.select("HomeTeam", "AwayTeam", "TotalGoals").show(n=cau4.count(), truncate=False)

# 3.7. Tìm những trận của Burnley được thi đấu trên sân nhà và có số bàn thắng >=3 (Tính cả của đội khách)
def cau5():
    sum_goals = data.withColumn("TotalGoals", col("FTHG") + col("FTAG"))
    cau5 = sum_goals.filter((col("HomeTeam") == "Burnley") & (col("TotalGoals") >= 3))
    print("Câu 3.7: Những trận của Burnley thi đấu trên sân nhà và có số bàn thắng >= 3 (cả đội khách):" + str(cau5.count()))
    cau5.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "TotalGoals").show(n=cau5.count(), truncate=False)
# 3.8. Tìm những trận mà Reading thua (Không được sử dụng cột FTR)
def cau6():
    homeTeam = data.filter((col("HomeTeam") == "Reading") & (col("FTHG") < col("FTAG")))\
                    .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG", "HTR")
    awayTeam = data.filter((col("AwayTeam") == "Reading") & (col("FTAG") > col("FTHG")))\
                    .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG","HTHG", "HTAG", "HTR")
    cau6 = homeTeam.union(awayTeam)
    # # Hiển thị danh sách các trận mà Reading thua với các cột đã chọn
    print("Câu 3.8: Những trận mà đội Reading thua")
    cau6.show()
# 3.9. Xoay giá trị trong cột FTR thành các cột, với mỗi cột chứa số lượng FTR tương ứng. nhóm theo HomeTeam
def cau7():
    cau7 = data.groupBy("HomeTeam", "FTR").agg(count("FTR").alias("Count"))
    cau7 = cau7.groupBy("HomeTeam").pivot("FTR").agg(sum("Count")).na.fill(0)
    # print("so cot"+str(cau9.count()))
    print("Câu 3.9: ")
    cau7.show(n=cau7.count(), truncate=False)
# 3.10. Tạo một cột mới với tên cột tuỳ chọn: Nếu tổng số bàn thắng 2 đội ghi được trong trận  <2 thì điền “well” , nếu số bàn thắng  2 < x < 4 thì điền “very good”, nếu số bàn thắng >= 4 thì điền “amazing”.
def cau8():
    cau8 = data.withColumn(
        "GoalsTotal",
        data["FTHG"] + data["FTAG"]
    )
    # Tính toán cột "Status" dựa trên "GoalsTotal"
    cau8 = cau8.withColumn(
        "Status",
        when(cau8["GoalsTotal"] < 2, "well")
        .when((cau8["GoalsTotal"] >= 2) & (cau8["GoalsTotal"] < 4), "very good")
        .otherwise("amazing")
    )
    # Hiển thị kết quả
    print("Câu 3.10")
    cau8.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "GoalsTotal", "Status").show(n=cau8.count(), truncate=False)

def main_menu():
    while True:
        try:
            print("\n<<<<<<<<<<<<<<<<<<< Menu: Câu3 - 71DCTT21 - Nhóm6 >>>>>>>>>>>>>>>>>>>")
            print("1. Có bao nhiêu đội đá trong mùa giải, liệt kê các đội")
            print("2. Tìm số trận có kết quả hoà")
            print("3. Tìm tổng số bàn thắng các đội đá trên sân nhà ghi được?")
            print("4. Tìm những trận có tổng số bàn thắng > 3")
            print("5.  Tìm những trận của Burnley được thi đấu trên sân nhà và có số bàn thắng >=3 (Tính cả của đội khách)")
            print("6. Tìm những trận mà Reading thua (Không được sử dụng cột FTR)")
            print("7. Xoay giá trị trong cột FTR thành các cột, với mỗi cột chứa số lượng FTR tương ứng. nhóm theo HomeTeam")
            print("8. Tạo một cột mới với tên cột tuỳ chọn: Nếu tổng số bàn thắng 2 đội ghi được trong trận  <2 thì điền “well” , nếu số bàn thắng  2 < x < 4 thì điền “very good”, nếu số bàn thắng >= 4 thì điền “amazing”")
            print("0. Thoát")
            key = int(input("Nhập số tùy chọn: "))
            print("\n")
            if   key == 1: cau1()
            elif key == 2: cau2()
            elif key == 3: cau3()
            elif key == 4: cau4()
            elif key == 5: cau5()
            elif key == 6: cau6()
            elif key == 7: cau7()
            elif key == 8: cau8()
            elif key == 0:
                print("Kết thúc chương trình.")
                break
            else:
                print("Tùy chọn không hợp lệ.")
        except ValueError:
            print("Vui lòng nhập một số nguyên.")
        input("\n\nNhấn Enter để tiếp tục...")
        

if __name__ == "__main__":
    main_menu()
