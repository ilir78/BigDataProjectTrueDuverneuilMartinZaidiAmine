import json, os
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import functions as F, types as T
from .common import PROCESSED, RAW, spark

def run():
    s=spark("Wikipedia changes for IMDb")
    servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
    topic=os.getenv("KAFKA_TOPIC","wiki-recentchange")
    schema=T.StructType([T.StructField("id",T.LongType()),T.StructField("type",T.StringType()),T.StructField("namespace",T.IntegerType()),T.StructField("title",T.StringType()),T.StructField("timestamp",T.LongType()),T.StructField("user",T.StringType()),T.StructField("comment",T.StringType()),T.StructField("server_url",T.StringType()),T.StructField("wiki",T.StringType())])
    # Broad English Wikipedia movie/series/person-title criteria ensure enough events in a short run.
    stream=(s.readStream.format("kafka").option("kafka.bootstrap.servers",servers).option("subscribe",topic).option("startingOffsets","latest").load()
            .select(F.from_json(F.col("value").cast("string"),schema).alias("e"),F.col("timestamp").alias("kafka_timestamp"))
            .select("e.*","kafka_timestamp").filter((F.col("wiki")=="enwiki") & (F.col("namespace")==0) & F.col("title").isNotNull() & F.col("id").isNotNull())
            .withColumn("title_lc",F.lower("title"))
            .filter(F.col("title_lc").rlike("(film|movie|television|actor|actress|director|cinema|series|imdb)"))
            .withColumn("event_time",F.to_timestamp(F.from_unixtime("timestamp")))
            .withWatermark("event_time","10 minutes").dropDuplicates(["id","event_time"]))
    checkpoint=os.getenv("SPARK_CHECKPOINT_DIR","data/checkpoints/wiki-imdb")
    metrics=PROCESSED/"stream_metrics.jsonl"
    def save_batch(batch_df,batch_id):
        count=batch_df.count()
        if count:
            batch_df.write.mode("append").parquet(str(PROCESSED/"wiki_changes"))
        record={"batch_id":batch_id,"processed_rows":count,"observed_at":datetime.now(timezone.utc).isoformat()}
        metrics.parent.mkdir(parents=True,exist_ok=True)
        with metrics.open("a",encoding="utf-8") as handle:
            handle.write(json.dumps(record)+"\n")
        print(json.dumps(record),flush=True)
    query=(stream.writeStream.foreachBatch(save_batch).option("checkpointLocation",checkpoint)
           .outputMode("append").trigger(processingTime="10 seconds").start())
    query.awaitTermination()
