from pyspark.sql.functions import *
from pyspark.sql.types import * 
import dlt

@dlt.view(
    name="trans_passengers"
)
def trans_passengers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")\
        .withColumn("modified_date",current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    stored_as_scd_type=1,
    sequence_by="modified_date"
)