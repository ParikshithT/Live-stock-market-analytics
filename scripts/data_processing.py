import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, mean, stddev
from sqlalchemy import create_engine

# Load configuration from config.json
with open('/app/config.json') as config_file:
    config = json.load(config_file)

# Database credentials
db_user = config['POSTGRES_USER']
db_password = config['POSTGRES_PASSWORD']
db_name = config['POSTGRES_DB']
db_host = 'postgres'  # This is the service name in docker-compose.yml

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("StockNewsDataProcessing") \
        .config("spark.jars", "jdbc_drivers/postgresql-42.7.3.jar") \
        .getOrCreate()

    # Load stock data
    stock_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:5432/{db_name}") \
        .option("dbtable", "stock_data") \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()

    # Load news data
    news_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:5432/{db_name}") \
        .option("dbtable", "news_data") \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()

    # Data cleaning and transformation for stock data
    stock_df = stock_df.dropDuplicates()
    stock_df = stock_df.na.fill({'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0})
    stock_df = stock_df.withColumn('year', year(col('timestamp'))) \
                       .withColumn('month', month(col('timestamp'))) \
                       .withColumn('day', dayofmonth(col('timestamp'))) \
                       .withColumn('hour', hour(col('timestamp'))) \
                       .withColumn('minute', minute(col('timestamp')))

    # Normalization of numerical features
    numerical_features = ['open', 'high', 'low', 'close', 'volume']
    for feature in numerical_features:
        mean_val = stock_df.select(mean(col(feature))).first()[0]
        stddev_val = stock_df.select(stddev(col(feature))).first()[0]
        stock_df = stock_df.withColumn(f'{feature}_normalized', (col(feature) - mean_val) / stddev_val)

    # Data cleaning and transformation for news data
    news_df = news_df.dropDuplicates()
    news_df = news_df.na.fill({'title': 'No Title', 'description': 'No Description', 'content': 'No Content'})
    news_df = news_df.withColumn('year', year(col('published_at'))) \
                     .withColumn('month', month(col('published_at'))) \
                     .withColumn('day', dayofmonth(col('published_at'))) \
                     .withColumn('hour', hour(col('published_at'))) \
                     .withColumn('minute', minute(col('published_at')))

    # Store cleaned data back into PostgreSQL
    stock_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:5432/{db_name}") \
        .option("dbtable", "stock_data_cleaned") \
        .option("user", db_user) \
        .option("password", db_password) \
        .mode("overwrite") \
        .save()

    news_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:5432/{db_name}") \
        .option("dbtable", "news_data_cleaned") \
        .option("user", db_user) \
        .option("password", db_password) \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    main()