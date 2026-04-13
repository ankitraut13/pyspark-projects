from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count

spark = SparkSession.builder \
    .appName("OMS Sellers Example") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

url = "jdbc:postgresql://10.10.0.4:5432/rapidshyp_uat"

properties = {
    "user": "Abcd",
    "password": "Abcdefg",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=url, table="oms.sellers", properties=properties)

#print("Original Data:")
#df.show(5)

df3 = (df.select("_id", "first_name", "seller_code")
       .withColumn("new_seller_code", col("seller_code") * 2)
       .filter(col("seller_code") < 24000183)
       .groupBy("seller_code")
       .agg(count("seller_code").alias("cnt"))
       )
df3.show(5)

#df4 = df3.withColumn("new_seller_code", col("seller_code") * 2)
#df4.show()

#df5 = df4.filter(col("seller_code") < 24000183)
#df5.show()

spark.stop()
print("test")