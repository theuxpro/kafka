import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date


schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])


@psf.udf(StringType())
def udf_convert_time(timestamp):
    conv_data = parse_date(timestamp)
    return str(conv_data.strftime('%y%m%d%H'))


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "service-calls").option("maxOffsetsPerTrigger", 200).option("startingOffsets", "earliest").load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")

    distinct_table = service_table\
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    counts_df = distinct_table.withWatermark("call_datetime", "60 minutes").groupBy('original_crime_type_name')

    converted_df = distinct_table.select(psf.col('crime_id'), psf.col('original_crime_type_name'), udf_convert_time(psf.col('call_datetime')))

    calls_per_2_days = counts_df.groupBy(psf.window(distinct_table.call_datetime, "2 days")).count()

    query = converted_df.writeStream.outputMode("Complete").format("console").start()

    query.awaitTermination()


if __name__ == "__main__":

    logger = logging.getLogger(__name__)
    spark = SparkSession.builder.master("local").appName("SF Crime Spark").getOrCreate()
    logger.info("Spark started")
    run_spark_job(spark)
    spark.stop()



