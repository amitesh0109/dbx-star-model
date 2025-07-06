from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")\
        .withColumn("amount",col("amount").cast(DoubleType()))\
        .withColumn("modified_date",current_timestamp())\
        .withColumn("booking_date",to_date(col("booking_date")))\
        .drop("_rescued_data")

    return df

rules = {
    "rule1":"booking_id IS NOT NULL",
    "rule2":"passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df