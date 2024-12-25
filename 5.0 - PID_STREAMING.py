# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC ## PID STREAMING
# MAGIC ---
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 20px;">
# MAGIC   <img src="https://raw.githubusercontent.com/animrichter/BDT_2023/master/data/assets/streaming.png" style="width: 1200">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cíl cvičení: 
# MAGIC - Transformovat řešení batch na streaming
# MAGIC - Číst PID data pomocí kafka consumeru
# MAGIC - Z načtených dat identifikovat atributy (1. , 2. , ..) https://api.golemio.cz/v2/pid/docs/openapi/#/
# MAGIC - Vytvořit tabulky Bronze - Silver
# MAGIC - Gold: Rozdělení podle typu prostředku / průměrné zpoždění / celkové, kolik km ujede jeden bus za den...)
# MAGIC - z Gold tabulek vytvořit přehledný dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Fields
# MAGIC
# MAGIC ### 1. `geometry.coordinates`
# MAGIC - **Description**: Coordinates representing the location of the vehicle.
# MAGIC
# MAGIC ### 2. `properties.trip.vehicle_type.description_en`
# MAGIC - **Description**: Description of the vehicle type in English.
# MAGIC   - **Note**: If empty, it represents a train.
# MAGIC
# MAGIC ### 3. `properties.trip.agency_name.scheduled`
# MAGIC - **Description**: Agency name that currently operates the trip
# MAGIC
# MAGIC ### 4. `properties.trip.gtfs.trip_id`
# MAGIC - **Description**: Identifier of the trip in the GTFS Static feed.
# MAGIC
# MAGIC ### 5. `properties.trip.vehicle_registration_number`
# MAGIC - **Description**: Four-digit identifier of the vehicle in the system.
# MAGIC
# MAGIC ### 6. `properties.trip.gtfs.route_short_name`
# MAGIC - **Description**: Identification of the line used for the public.
# MAGIC
# MAGIC ### 7. `properties.last_position.delay.actual`
# MAGIC - **Description**: Current delay, in seconds.
# MAGIC
# MAGIC ### 8. `properties.last_position.origin_timestamp`
# MAGIC - **Description**: Time at which the position was sent from the vehicle (UTC).
# MAGIC
# MAGIC ### 9. `properties.last_position.shape_dist_traveled`
# MAGIC - **Description**: Number of kilometers traveled on the route.

# COMMAND ----------

kafka_cluster = "b-3-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196,b-2-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196,b-1-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196"

topic = "fel-pid-topic"

# COMMAND ----------

schema = 'array<struct<geometry: struct<coordinates: array<double>, type: string>, properties: struct<last_position: struct<bearing: int, delay: struct<actual: int, last_stop_arrival: int, last_stop_departure: int>, is_canceled: string, last_stop: struct<arrival_time: string, departure_time: string, id: string, sequence: int>, next_stop: struct<arrival_time: string, departure_time: string, id: string, sequence: int>, origin_timestamp: string, shape_dist_traveled: string, speed: string, state_position: string, tracking: boolean>, trip: struct<agency_name: struct<real: string, scheduled: string>, cis: struct<line_id: string, trip_number: string>, gtfs: struct<route_id: string, route_short_name: string, route_type: int, trip_headsign: string, trip_id: string, trip_short_name: string>, origin_route_name: string, sequence_id: int, start_timestamp: string, vehicle_registration_number: string, vehicle_type: struct<description_cs: string, description_en: string, id: int>, wheelchair_accessible: boolean, air_conditioned: boolean, usb_chargers: boolean>>, type: string>>'

# COMMAND ----------


# Read stream
raw = (
    spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', kafka_cluster)
        .option('subscribe', topic)
        .option('startingOffsets', "earliest")
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.jaas.config', 'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="FELPIDUSER" password="dzs3c1vldy6np5";')
        .load()
)

checkpoint = '/mnt/pid/checkpoint_file.txt'

# Write to brozne table
(raw.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint)
   .toTable("fel_pid_topic_data")
)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import from_json, col, explode, size, to_timestamp

bronze_df_stream = spark.readStream.table("fel_pid_topic_data")

# Parse binary value to schema
parsed_df = (
    bronze_df_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    # Filter out rows where 'data' is null or empty
    .filter(col("data").isNotNull() & (size(col("data")) > 0))
)

# Flatten the array
exploded_df = parsed_df.select(explode(col("data")).alias("root"))

# Create silver table, flatten the struct
df_silver_stream = (
    exploded_df
    .select(
        col("root.geometry.coordinates")[0].alias("x_cord"),
        col("root.geometry.coordinates")[1].alias("y_cord"),
        col("root.properties.trip.vehicle_type.description_en").alias("vehicle_type_description_en"),
        col("root.properties.trip.agency_name.scheduled").alias("agency_name_scheduled"),
        col("root.properties.trip.gtfs.trip_id").alias("gtfs_trip_id"),
        col("root.properties.trip.vehicle_registration_number").alias("vehicle_registration_number"),
        col("root.properties.trip.gtfs.route_short_name").alias("route_short_name"),
        col("root.properties.last_position.delay.actual").alias("delay_actual"),
        to_timestamp(col("root.properties.last_position.origin_timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("origin_timestamp"),
        col("root.properties.last_position.shape_dist_traveled").cast("double").alias("shape_dist_traveled")
    )
)

silver_checkpoint_path = '/mnt/pid/silver_checkpoint'

# Save the silver table
df_silver_stream.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", silver_checkpoint_path) \
    .option("mergeSchema", "true") \
    .toTable("silver_pid_table")

# COMMAND ----------

from pyspark.sql.functions import window, avg, round 

df_silver_stream = spark.readStream.table("silver_pid_table") 

df_gold_average_delay_by_type = (
    df_silver_stream
    .groupBy("vehicle_type_description_en")
    .agg(avg("delay_actual").alias("avg_delay"))
    .where("vehicle_type_description_en != 'NULL'")
    .select(
        col("vehicle_type_description_en").alias("vehicle_type"),
        col("avg_delay")
    )
)

gold_avg_delay_checkpoint = "/mnt/pid/gold_avg_delay_checkpoint"

df_gold_average_delay_by_type.writeStream.format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", gold_avg_delay_checkpoint)\
    .option("mergeSchema", "true") \
    .toTable("gold_pid_table_avg_delay_by_type")

df_gold_avg_delay_2_hours_interval = (
    df_silver_stream
    .withWatermark("origin_timestamp", "4 hours")
    .groupBy(
        "vehicle_type_description_en",
        window("origin_timestamp", "2 hours").alias("time_window")
    )
    .agg(avg("delay_actual").alias("avg_delay"))
    .where("vehicle_type_description_en != 'NULL'")
    .select(
        col("vehicle_type_description_en").alias("vehicle_type"),
        col("time_window.start").alias("from"),
        col("time_window.end").alias("to"),
        col("avg_delay")
    )
)

gold_2h_checkpoint = "/mnt/pid/gold_2h_interval_checkpoint"

df_gold_avg_delay_2_hours_interval.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", gold_2h_checkpoint)\
    .option("mergeSchema", "true") \
    .toTable("gold_pid_table_avg_delay_interval")

df_silver_with_places = df_silver_stream.select(
    round(col("x_cord"), 3).alias("x_cord_rounded"),
    round(col("y_cord"), 3).alias("y_cord_rounded"),
    col("vehicle_type_description_en"),
    col("delay_actual")
)

df_grouped_by_place_and_type = (
    df_silver_with_places
    .groupBy("vehicle_type_description_en", "x_cord_rounded", "y_cord_rounded")
    .agg(avg("delay_actual").alias("avg_delay"))
    .where("vehicle_type_description_en != 'NULL'")
)

gold_places_checkpoint = "/mnt/pid/gold_places_checkpoint"

df_grouped_by_place_and_type.writeStream.format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", gold_places_checkpoint) \
    .toTable("gold_pid_table_places_with_biggest_avg_delay")


# COMMAND ----------



# COMMAND ----------

# ChatGPT o1 model was used

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC =======================================================================================================================================
# MAGIC
# MAGIC Zadání A: Proveďte analýzu zpoždění různých typů dopravních prostředků (autobusy, tramvaje, metro, vlaky) v průběhu celého dne. Porovnejte průměrné zpoždění pro každý typ vozidla a zjistěte, jak se mění v 2 hod. časových intervalech. Výsledky zobrazte v grafu. Jako další krok identifikujte 3 místa s největším průměrným zpožděním pro daný typ dopravního prostředku a vizualizujte je na mapě.
# MAGIC Cíl: Odhalit vzorce v zpoždění různých typů dopravních prostředků a lokalizovat klíčová místa, kde k nim dochází.
# MAGIC
# MAGIC ============================================================================================================================
# MAGIC
# MAGIC Zadání B: Proveďte analýzu zpoždění různých typů dopravních prostředků (autobusy, tramvaje, metro, vlaky) v průběhu celého dne. Porovnejte průměrné zpoždění pro každý typ vozidla a zjistěte, jak se mění v 3 hod. časových intervalech. Výsledky zobrazte v grafu. Jako další krok identifikujte 3 místa s nejmenším průměrným zpožděním pro daný typ dopravního prostředku a vizualizujte je na mapě.
# MAGIC Cíl: Odhalit vzorce v zpoždění různých typů dopravních prostředků a lokalizovat klíčová místa, kde k nim dochází.
# MAGIC
# MAGIC =============================================================================================================================