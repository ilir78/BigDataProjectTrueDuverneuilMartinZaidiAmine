from pathlib import Path
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
RESULTS = ROOT / "reports" / "results"
TABLES = {
    "akas": "title.akas.tsv.gz", "basics": "title.basics.tsv.gz",
    "crew": "title.crew.tsv.gz", "episode": "title.episode.tsv.gz",
    "principals": "title.principals.tsv.gz", "ratings": "title.ratings.tsv.gz",
    "names": "name.basics.tsv.gz",
}

def spark(app: str) -> SparkSession:
    builder=(SparkSession.builder.appName(app).master(os.getenv("SPARK_MASTER","local[4]"))
             .config("spark.driver.memory",os.getenv("SPARK_DRIVER_MEMORY","4g"))
             .config("spark.sql.shuffle.partitions",os.getenv("SPARK_SHUFFLE_PARTITIONS","48"))
             .config("spark.sql.session.timeZone", "UTC"))
    if app == "Wikipedia changes for IMDb":
        builder=builder.config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    return builder.getOrCreate()

def read_table(session: SparkSession, key: str):
    path = RAW / TABLES[key]
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}; download the official IMDb TSV dump first")
    df=session.read.option("header", True).option("sep", "\t").option("nullValue", "\\N").csv(str(path))
    return df.select(*[F.when(F.trim(F.col(c))=="",F.lit(None)).otherwise(F.col(c)).alias(c) for c in df.columns])

def write_csv(df, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            df=df.withColumn(field.name,F.concat_ws(", ",F.col(field.name)))
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path))

def write_parquet(df, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(path))
