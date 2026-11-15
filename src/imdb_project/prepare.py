from pyspark.sql import functions as F
from .common import TABLES, RAW, PROCESSED, RESULTS, read_table, spark, write_csv, write_parquet

def _complete(path):
    return (path / "_SUCCESS").exists()

def _write_parquet_once(df, path):
    if _complete(path):
        print(f"Already complete, keeping {path}")
    else:
        write_parquet(df, path)

def run():
    s = spark("IMDb preparation")
    frames = {k: read_table(s, k) for k in TABLES}
    null_path = RESULTS / "null_rates.csv"
    top_null_path = RESULTS / "top_null_columns.csv"
    if not (_complete(null_path) and _complete(top_null_path)):
        # IMDb encodes unavailable values as \\N; whitespace-only strings are treated as empty too.
        stats = []
        for table, df in frames.items():
            expressions=[F.count(F.lit(1)).alias("__rows")]
            expressions += [F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c))==""),1).otherwise(0)).alias("m"+str(i)) for i,c in enumerate(df.columns)]
            values=df.agg(*expressions).first().asDict()
            total=values["__rows"] or 0
            for i,col in enumerate(df.columns):
                n=values.get("m"+str(i)) or 0
                stats.append((table,col,total,n,100.0*n/total if total else 0.0))
        # Build a tiny Spark SQL relation from the aggregate scalars, avoiding Python RDD workers.
        selects=[]
        for table,col,total,n,pct in stats:
            safe_table=table.replace("'","''")
            safe_col=col.replace("'","''")
            selects.append(f"SELECT '{safe_table}' AS table_name, '{safe_col}' AS column_name, {int(total)}L AS row_count, {int(n)}L AS missing_count, {float(pct)}D AS missing_pct")
        null_rates = s.sql(" UNION ALL ".join(selects))
        if not _complete(null_path):
            write_csv(null_rates.orderBy("table_name", "column_name"), null_path)
        if not _complete(top_null_path):
            write_csv(null_rates.orderBy(F.desc("missing_pct")).limit(5), top_null_path)
    else:
        print("Null-rate reports already complete; reusing existing results.")

    # Canonicalize keys; do not invent unknown values. Invalid or orphaned rows are audited separately.
    issues = RESULTS / "quality_issues"
    basics_path = PROCESSED / "silver" / "title_basics"
    names_path = PROCESSED / "silver" / "name_basics"
    ratings_path = PROCESSED / "silver" / "title_ratings"
    if _complete(basics_path):
        basics=s.read.parquet(str(basics_path))
    else:
        basics = (frames["basics"].dropDuplicates(["tconst"]).filter(F.col("tconst").isNotNull() & (F.trim("tconst")!=""))
                  .withColumn("startYear",F.col("startYear").cast("int")).withColumn("endYear",F.col("endYear").cast("int"))
                  .withColumn("runtimeMinutes",F.col("runtimeMinutes").cast("int")).withColumn("genres",F.split(F.col("genres"),",")))
        _write_parquet_once(basics,basics_path)
    if _complete(names_path):
        names=s.read.parquet(str(names_path))
    else:
        names = (frames["names"].dropDuplicates(["nconst"]).filter(F.col("nconst").isNotNull() & (F.trim("nconst")!=""))
                 .withColumn("birthYear",F.col("birthYear").cast("int")).withColumn("deathYear",F.col("deathYear").cast("int"))
                 .withColumn("primaryProfession",F.split(F.col("primaryProfession"),","))
                 .withColumn("knownForTitles",F.split(F.col("knownForTitles"),",")))
        invalid_people=names.filter(F.col("birthYear").isNotNull() & F.col("deathYear").isNotNull() & (F.col("deathYear")<F.col("birthYear")))
        _write_parquet_once(invalid_people,issues/"death_before_birth")
        names=names.join(invalid_people.select("nconst"),"nconst","left_anti")
        _write_parquet_once(names,names_path)
    if _complete(ratings_path):
        ratings=s.read.parquet(str(ratings_path))
    else:
        invalid_ratings=frames["ratings"].withColumn("average",F.col("averageRating").cast("double")).withColumn("votes",F.col("numVotes").cast("long")).filter((F.col("average").isNull())|(~F.col("average").between(1,10))|(F.col("votes").isNull())|(F.col("votes")<0))
        _write_parquet_once(invalid_ratings,issues/"invalid_ratings")
        ratings = (frames["ratings"].dropDuplicates(["tconst"]).withColumn("averageRating",F.col("averageRating").cast("double"))
                   .withColumn("numVotes",F.col("numVotes").cast("long")).filter(F.col("tconst").isNotNull() & F.col("averageRating").between(1,10) & (F.col("numVotes")>=0)))
        _write_parquet_once(ratings,ratings_path)

    # Foreign key audits are evaluated against deduplicated parent keys.
    title_keys = basics.select("tconst").distinct()
    name_keys = names.select("nconst").distinct()
    _write_parquet_once(frames["principals"].join(title_keys, "tconst", "left_anti"), issues / "principals_orphan_titles")
    _write_parquet_once(frames["principals"].join(name_keys, "nconst", "left_anti"), issues / "principals_orphan_names")
    _write_parquet_once(frames["akas"].join(title_keys.withColumnRenamed("tconst","titleId"),"titleId","left_anti"), issues / "akas_orphan_titles")
    _write_parquet_once(frames["episode"].join(title_keys,"tconst","left_anti"), issues / "episodes_orphan_titles")
    _write_parquet_once(frames["episode"].join(title_keys.withColumnRenamed("tconst", "parentTconst"), "parentTconst", "left_anti"), issues / "episodes_orphan_series")
    _write_parquet_once(frames["crew"].join(title_keys, "tconst", "left_anti"), issues / "crew_orphan_titles")
    for role_col in ("directors","writers"):
        role_edges=frames["crew"].select("tconst",F.explode_outer(F.split(role_col,",")).alias("nconst")).filter(F.col("nconst").isNotNull())
        _write_parquet_once(role_edges.join(name_keys,"nconst","left_anti"),issues/("crew_orphan_"+role_col))
    known_for=names.select("nconst",F.explode_outer("knownForTitles").alias("tconst")).filter(F.col("tconst").isNotNull())
    _write_parquet_once(known_for.join(title_keys,"tconst","left_anti"),issues/"names_orphan_known_for_titles")
    s.stop()
