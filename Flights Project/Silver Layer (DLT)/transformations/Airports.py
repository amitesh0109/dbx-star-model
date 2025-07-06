from pyspark.sql.functions import *
from pyspark.sql.types import * 
import dlt

@dlt.view(
    name="trans_airports"
)
def trans_passengers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")\
        .withColumn("modified_date",current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    stored_as_scd_type=1,
    sequence_by="modified_date"
)