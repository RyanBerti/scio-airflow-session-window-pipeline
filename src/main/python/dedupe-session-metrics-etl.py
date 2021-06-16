import argparse
import datetime

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_bucket", required=True)
parser.add_argument("--event_time_second", type=int, required=True)
parser.add_argument("--output_bucket", required=True)

args = parser.parse_args()
input_bucket = args.input_bucket
event_time_second = args.event_time_second
output_bucket = args.output_bucket

spark = SparkSession \
    .builder \
    .appName("Deduplicate session metrics") \
    .getOrCreate()

partition_path = datetime.datetime.fromtimestamp(event_time_second).strftime("y=%Y/m=%m/d=%d/h=%H/m=%M")
spark.read.parquet(f"{input_bucket}/{partition_path}").createOrReplaceTempView("event_time_partition")
spark.sql("""

    with row_numbered as (
        select
            *,
            row_number() over (partition by sessionId order by rt desc) as row_number
        from
            event_time_partition
    )
    
    select
        *
    from
        row_numbered
    where
        row_number = 1

""").drop('row_number').write.mode('overwrite').parquet(f"{output_bucket}/{partition_path}")