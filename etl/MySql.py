# ============================
# Lớp hỗ trợ MySQL
# ============================
class MySql:
    # ----------------------------
    # Khởi tạo kết nối
    # ----------------------------
    def __init__(self, spark):
        self.spark = spark
        # self.url = "jdbc:mysql://localhost:3307/DE_2025_data_warehouse"
        self.url = "jdbc:mysql://mysql_dwh:3306/DE_2025_data_warehouse"  # URL kết nối tới MySQL container
        self.user = "root"
        self.password = "123"
        self.driver = "com.mysql.cj.jdbc.Driver"

    # ----------------------------
    # Đọc dữ liệu từ bảng MySQL
    # ----------------------------
    def read(self, table):
        df = (
            self.spark.read.format("jdbc")
            .option("url", self.url)
            .option("dbtable", table)
            .option("user", self.user)
            .option("password", self.password)
            .option("driver", self.driver)
            .load()
        )
        return df

    # ----------------------------
    # Ghi DataFrame vào MySQL (overwrite)
    # ----------------------------
    def insert(self, table, df):
        try:
            df.write.format("jdbc").option("url", self.url).option(
                "dbtable", table
            ).option("user", self.user).option("password", self.password).option(
                "driver", self.driver
            ).mode(
                "overwrite"
            ).save()

            return True

        except Exception as e:
            print("Insert random error:", e)
            return False
