from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, min, max

def main():
    # Create Spark session
    spark = SparkSession.builder \
    .appName("BatchAggregation") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("spark.hadoop.native.lib", "false") \
    .getOrCreate()


    # Read input CSV
    df = spark.read.csv("input/metrics.csv", header=True, inferSchema=True)

    # Add 'date' column for daily bucketing
    df = df.withColumn("date", to_date(col("timestamp")))

    result = df.groupBy("metric", "date") \
        .agg(
            avg("value").alias("average_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value")
        ) \
        .orderBy("metric", "date")

    # Write output to CSV
    result.toPandas().to_csv("output/aggregated_metrics.csv", index=False)

    spark.stop()

if __name__ == "__main__":
    main()
