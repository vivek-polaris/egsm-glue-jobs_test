import sys
import logging
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, when, unix_timestamp, lit, date_format,
    from_json, expr, from_utc_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.window import Window

# Initialize Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job.init(args["JOB_NAME"], args)

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(name)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# Constants
EVENT_OFF = 101
EVENT_ON = 102
OUTAGE_DURATION_THRESHOLD = 24 * 60 * 60

block_load_schema = StructType([
    StructField("datetime", StringType()),
    StructField("data_type", StringType()),
    StructField("temperature", DoubleType()),
    StructField("Average_voltage", DoubleType()),
    StructField("Average_Current_IP", DoubleType()),
    StructField("Block_energy_kWh_Export", DoubleType()),
    StructField("Block_energy_kWh_Import", DoubleType()),
    StructField("Block_energy_kVAh_Export", DoubleType()),
    StructField("Block_energy_kVAh_Import", DoubleType()),
    StructField("Cumulative_energy_kWh_Export", DoubleType()),
    StructField("Cumulative_energy_kWh_Import", DoubleType()),
    StructField("Cumulative_energy_kVAh_Export", DoubleType()),
    StructField("Cumulative_energy_kVAh_Import", DoubleType()),
    StructField("Real_time_clock_date_and_time", StringType())
])

def get_device_identifiers():
    # query = """SELECT device_id FROM public.sat1 WHERE tag='sat1' ORDER BY device_id OFFSET 23000"""
    query = """SELECT device_id FROM public.sat1 WHERE tag='sat1' ORDER BY device_id LIMIT 5000 OFFSET 5000"""
    df = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.sat1",
            "connectionName": "MVVNL-Saryu-RoutingService",
            "sampleQuery": query,
            "enablePartitioningForSampleQuery": False
        }
    ).toDF()
    return [row['device_id'] for row in df.collect()]

def read_meter_pull_events(device_ids, start_time, end_time):
    # query = f"""
    #     SELECT * FROM public.meter_pull_event_logs
    #     WHERE device_id IN ({','.join(f"'{i}'" for i in device_ids)})
    # """
    
    query = f"""
    SELECT data_timestamp, device_id, event_code
    FROM public.meter_pull_event_logs
    WHERE device_id IN ({','.join(f"'{i}'" for i in device_ids)})
    AND data_timestamp BETWEEN '{start_time}' AND '{end_time}'
    """
    
    df = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.meter_pull_event_logs",
            "connectionName": "MVVNL-Saryu-RoutingService",
            "sampleQuery": query,
            "enablePartitioningForSampleQuery": False
        }
    ).toDF()
    df = df.withColumn("data_timestamp", from_utc_timestamp(col("data_timestamp"), "Asia/Kolkata"))
    return df.filter((col("event_code") == EVENT_OFF) | (col("event_code") == EVENT_ON)).select(
        col("device_id").alias("meter_id"),
        col("data_timestamp").alias("event_time"),
        col("event_code")
    ).orderBy("meter_id", "event_time", "event_code")

def read_meter_block_loads(device_ids,start_time, end_time):
    # query = f"""
    #     SELECT * FROM public.meter_block_loads
    #     WHERE device_id IN ({','.join(f"'{i}'" for i in device_ids)})
    # """
    
    query = f"""
    SELECT data_timestamp, device_id, data->>'Average_voltage' AS average_voltage
    FROM public.meter_block_loads
    WHERE device_id IN ({','.join(f"'{i}'" for i in device_ids)})
    AND data_timestamp BETWEEN '{start_time}' AND '{end_time}'"""
    
    df = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.meter_block_loads",
            "connectionName": "MVVNL-Saryu-RoutingService",
            "sampleQuery": query,
            "enablePartitioningForSampleQuery": False
        }
    ).toDF()
    df = df.withColumn("data_timestamp", from_utc_timestamp(col("data_timestamp"), "Asia/Kolkata"))
    # df = df.withColumn("parsed_data", from_json(col("data"), block_load_schema))
    return df.select(
        col("device_id").alias("meter_id"),
        col("data_timestamp").alias("block_time"),
        col("average_voltage").alias("voltage")
    ).filter(col("voltage").isNotNull())

def process_power_events(event_df):
    window_spec = Window.partitionBy("meter_id").orderBy("event_time", "event_code")
    df = event_df.withColumn("prev_event_time", lag("event_time", 1).over(window_spec))
    df = df.withColumn("prev_event_code", lag("event_code", 1).over(window_spec))
    return df.filter((col("event_code") == EVENT_ON) & (col("prev_event_code") == EVENT_OFF) & col("prev_event_time").isNotNull()) \
        .withColumn("duration_seconds", unix_timestamp("event_time") - unix_timestamp("prev_event_time")) \
        .select(
            col("meter_id").alias("MeterId"),
            col("prev_event_time").alias("Power_Off_Datetime"),
            col("event_time").alias("First_power_ON_after_Power_Off_datetime"),
            col("duration_seconds")
        )

def handle_exceptions(event_df, block_df):
    window_spec = Window.partitionBy("meter_id").orderBy("event_time")
    df = event_df.withColumn("prev_event_time", lag("event_time").over(window_spec))
    df = df.withColumn("prev_event_code", lag("event_code").over(window_spec))

    exceptions = df.filter(
        ((col("event_code") == col("prev_event_code")) |
         ((col("event_code") == EVENT_ON) & (col("prev_event_code") == EVENT_OFF) &
          (unix_timestamp("event_time") - unix_timestamp("prev_event_time") > OUTAGE_DURATION_THRESHOLD))) &
        col("prev_event_time").isNotNull()
    ).select(
        col("meter_id").alias("MeterId"),
        col("prev_event_time").alias("Power_Off_Datetime"),
        col("event_time").alias("First_power_ON_after_Power_Off_datetime")
    )

    block_df = block_df.withColumn("voltage_flag", when(col("voltage") <= 5, lit(0)).otherwise(lit(1)))
    result_rows = []
    for row in exceptions.collect():
        meter_id = row["MeterId"]
        start_time = row["Power_Off_Datetime"]
        end_time = row["First_power_ON_after_Power_Off_datetime"]
        relevant_blocks = block_df.filter(
            (col("meter_id") == meter_id) &
            (col("block_time") >= start_time) &
            (col("block_time") <= end_time)
        ).orderBy("block_time").collect()

        outage_start = None
        valid_outage_found = False

        for blk in relevant_blocks:
            if blk["voltage_flag"] == 0 and outage_start is None:
                outage_start = blk["block_time"]
            elif blk["voltage_flag"] == 1 and outage_start is not None:
                result_rows.append((meter_id, outage_start, blk["block_time"]))
                valid_outage_found = True
                outage_start = None

        if not valid_outage_found:
            continue

    if not result_rows:
        return spark.createDataFrame([], StructType([
            StructField("MeterId", StringType()),
            StructField("Power_Off_Datetime", TimestampType()),
            StructField("First_power_ON_after_Power_Off_datetime", TimestampType()),
            StructField("duration_seconds", LongType())
        ]))

    df = spark.createDataFrame(result_rows, ["MeterId", "Power_Off_Datetime", "First_power_ON_after_Power_Off_datetime"])
    df = df.withColumn("duration_seconds", unix_timestamp("First_power_ON_after_Power_Off_datetime") - unix_timestamp("Power_Off_Datetime"))
    return df.filter(col("duration_seconds") > 0)

def format_output(df):
    return df.withColumn(
        "Duration (Days, HH:MM:SS)",
        expr("""
            concat(
                cast(floor(duration_seconds / 86400) as string),
                ' days, ',
                format_string('%02d:%02d:%02d',
                    floor((duration_seconds % 86400) / 3600),
                    floor((duration_seconds % 3600) / 60),
                    floor(duration_seconds % 60))
            )
        """
    )).withColumn("year", date_format(col("First_power_ON_after_Power_Off_datetime"), "yyyy")) \
     .withColumn("month", date_format(col("First_power_ON_after_Power_Off_datetime"), "MM")) \
     .select("MeterId", "Power_Off_Datetime", "First_power_ON_after_Power_Off_datetime", "Duration (Days, HH:MM:SS)", "year", "month")

def write_to_iceberg(df, table_name, s3_path):
    writer = df.writeTo(f"glue_catalog.db_missing_readings_blockload.{table_name}") \
        .tableProperty("format-version", "2") \
        .tableProperty("location", s3_path) \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .option("merge-schema", "true")
    if table_name in {t.name for t in spark.catalog.listTables("db_missing_readings_blockload")}:
        writer.append()
    else:
        writer.create()

def main():
    logger.info("Starting Power Outage Job")
    start_time = "2025-05-31 18:30:00"
    end_time = "2025-06-26 23:59:59"
    device_ids = get_device_identifiers()
    event_df = read_meter_pull_events(device_ids,start_time, end_time)
    block_df = read_meter_block_loads(device_ids,start_time, end_time)
    outage_df = process_power_events(event_df)
    verified_df = handle_exceptions(event_df, block_df)
    final_df = outage_df.unionByName(verified_df)
    logger.info(f"Total outages: {final_df.count()}")
    formatted = format_output(final_df)
    output_path = f"s3://polaris-analytics-helper/vivek/june_power_outage_saryu_v2/{datetime.now().strftime('%Y-%m-%d')}"
    write_to_iceberg(formatted, "june_power_outage_saryu_v2", output_path)
    logger.info("Job completed successfully")

if __name__ == "__main__":
    main()
    job.commit()
