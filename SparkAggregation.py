from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, dayofmonth, rank, month, col, desc
from pyspark.sql.window import Window
from Variables import *

def sparkCode():
    spark = SparkSession.builder \
            .config("spark.jars", "/home/bhupesh/Desktop/air_quality_index/jars/aws-java-sdk-bundle-1.11.375.jar,/home/bhupesh/Desktop/air_quality_index/jars/hadoop-aws-3.3.1.jar,/home/bhupesh/Desktop/air_quality_index/jars/mysql-connector-java-8.0.13.jar") \
            .config("spark.hadoop.fs.s3a.access.key",access_key )\
            .config("spark.hadoop.fs.s3a.secret.key",secret_key )\
            .getOrCreate()
    # spark.conf.set("spark.hadoop.metrics.ignoreConfigFiles", "true")
    df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", f"jdbc:mysql://{database_info['host']}:{database_info['port']}/airqualityindex?useSSL=false&trustServerCertificate=true") \
    .option("dbtable", database_info['table']) \
    .option("user", database_info['user']) \
    .option("password", database_info['password']) \
    .load()
    df.printSchema()

    result_df = df.withColumn("day", dayofmonth("last_update"))

    window_spec = Window().partitionBy("city","day", "pollutant_id")

    result_df = result_df.withColumn("avg_min", avg("pollutant_min").over(window_spec))
    result_df = result_df.withColumn("avg_max", avg("pollutant_max").over(window_spec))
    result_df = result_df.withColumn("avg_avg", avg("pollutant_avg").over(window_spec))
    result_df = result_df.withColumn("average_pollutant_level", (col("avg_min") + col("avg_max")) / 6)

    df2=result_df.select("id", "country", "state", "city", "station", "last_update", "pollutant_id", "day", "average_pollutant_level")

    # Display the first 10 rows and all original columns
    df2.show(10)

    partition_columns = ["pollutant_id", "day"]

    df2.write.format("parquet")\
        .mode("overwrite")\
        .partitionBy(partition_columns)\
        .option("compression","snappy")\
        .save(s3_output_location)

    spark.stop()

if __name__ == '__main__':
    sparkCode()