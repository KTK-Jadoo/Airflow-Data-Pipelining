# transform.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from datetime import datetime

def transform_data():
    spark = SparkSession.builder.appName("FinancialDataTransformation").getOrCreate()
    
    # Load raw data
    raw_data = spark.read.json('/path/to/hdfs/data/raw/*.json')
    
    # Flatten JSON structure and select relevant columns
    df = raw_data.selectExpr("`Time Series (Daily)`.*")
    
    # Convert column names to a more usable format
    df = df.withColumnRenamed("1. open", "open") \
           .withColumnRenamed("2. high", "high") \
           .withColumnRenamed("3. low", "low") \
           .withColumnRenamed("4. close", "close") \
           .withColumnRenamed("5. volume", "volume")
    
    # Cast columns to appropriate data types
    df = df.select(
        col("open").cast("float"),
        col("high").cast("float"),
        col("low").cast("float"),
        col("close").cast("float"),
        col("volume").cast("int")
    )
    
    # Add a new column for the date
    df = df.withColumn("date", col("Date").cast("date"))
    
    # Calculate moving averages
    df = df.withColumn("moving_average_50", avg("close").over(Window.orderBy("date").rowsBetween(-49, 0)))
    
    # Save transformed data
    df.write.mode("overwrite").json('/path/to/hdfs/data/processed')
    
    spark.stop()

if __name__ == "__main__":
    transform_data()
