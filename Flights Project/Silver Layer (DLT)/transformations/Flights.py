from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

@dlt.view(
    name = "trans_flights"
)
def trans_flights():
    df= spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/flights/data")\
    .withColumn("modified_date",current_timestamp())\
    .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights", 
    source ="trans_flights",
    keys = ["flight_id"],
    sequence_by="modified_date",
    stored_as_scd_type = 1
)