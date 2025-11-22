class MySql:
    def __init__(self, spark):
        self.spark = spark
        # self.url = "jdbc:mysql://localhost:3307/DE_2025_data_warehouse"
        self.url = "jdbc:mysql://mysql_dwh:3306/DE_2025_data_warehouse"
        self.user = "root"
        self.password = "123"
        self.driver = "com.mysql.cj.jdbc.Driver"

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
