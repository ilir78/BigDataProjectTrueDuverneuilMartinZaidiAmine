"""Collect human-authored Wikipedia text, then enforce the output schema with Spark."""
import json, os, time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
import requests
from pyspark.sql import functions as F, types as T, Window
from .common import RESULTS, read_table, spark, write_csv, write_parquet

API = "https://en.wikipedia.org/w/api.php"
RAW_JSONL = Path(__file__).resolve().parents[2] / "data" / "raw" / "wikipedia_enrichment.jsonl"

def _api(session, params):
    for attempt in range(6):
        response = session.get(API, params={**params, "format":"json", "formatversion":2}, timeout=30)
        if response.status_code in (429, 503):
            delay=response.headers.get("Retry-After")
            if delay and delay.isdigit():
                seconds=float(delay)
            elif delay:
                try: seconds=max(0,(parsedate_to_datetime(delay)-datetime.now(timezone.utc)).total_seconds())
                except (TypeError,ValueError): seconds=min(60,2**attempt)
            else:
                seconds=min(60,2**attempt)
            time.sleep(seconds)
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError("Wikipedia API remained rate limited after six retries")

def _search(session, query):
    data = _api(session,{"action":"query","list":"search","srsearch":query,"srlimit":1})
    hits=data.get("query",{}).get("search",[])
    return hits[0]["title"] if hits else None

def _page(session, query):
    title=_search(session,query)
    if not title: return None
    data=_api(session,{"action":"query","prop":"extracts|info","exintro":1,"explaintext":1,"inprop":"url","titles":title})
    pages=data.get("query",{}).get("pages",[])
    if not pages or pages[0].get("missing"): return None
    page=pages[0]
    return {"page_title":page.get("title"),"text":page.get("extract",""),"source_url":page.get("fullurl") or "https://en.wikipedia.org/wiki/"+page.get("title","").replace(" ","_")}

def run():
    # Select top ten by IMDb vote count within each release-year/genre. IMDb IDs anchor each person.
    s=spark("IMDb enrichment")
    basics=read_table(s,"basics").filter((F.col("titleType")=="movie")&(F.col("isAdult")=="0")&F.col("startYear").cast("int").between(1980,2025)&(F.col("runtimeMinutes").cast("int")>=40))
    ratings=read_table(s,"ratings").withColumn("votes",F.col("numVotes").cast("long"))
    exploded=basics.join(ratings,"tconst").withColumn("year",F.col("startYear").cast("int")).withColumn("genre",F.explode(F.split("genres",",")))
    w=Window.partitionBy("year","genre").orderBy(F.desc("votes"),F.asc("tconst"))
    selected=(exploded.withColumn("genre_rank",F.row_number().over(w)).filter("genre_rank <= 10")
              .select("tconst","primaryTitle","year","genre","genre_rank"))
    # Process all top-ten-per-year-and-genre titles by default. An optional cap
    # is available for quick smoke runs; it must not be set for the submission.
    max_films=os.getenv("WIKI_MAX_FILMS")
    if max_films:
        selected=selected.orderBy("year","genre","genre_rank","tconst").limit(int(max_films))
    principal=(read_table(s,"principals").filter(F.col("category").isin("actor","actress","director","writer"))
               .withColumn("person_rank",F.row_number().over(Window.partitionBy("tconst").orderBy("ordering")))
               .filter("person_rank <= 3").join(read_table(s,"names").select("nconst","primaryName"),"nconst")
               .join(selected.select("tconst").distinct(),"tconst","inner"))
    # Keep only completed entity IDs in driver memory. Collecting the complete
    # IMDb selection at once can exhaust the Spark driver's Java heap.
    films=selected.dropDuplicates(["tconst"]).select("tconst","primaryTitle").toLocalIterator()
    people=principal.select("tconst","nconst","primaryName","person_rank").dropDuplicates(["tconst","nconst"]).toLocalIterator()
    principal_counts=principal.groupBy("tconst").agg(F.countDistinct("nconst").alias("principal_count"))
    RAW_JSONL.parent.mkdir(parents=True,exist_ok=True)
    cached_ids=set()
    if RAW_JSONL.exists():
        for line in RAW_JSONL.read_text(encoding="utf-8").splitlines():
            try:
                cached_ids.add(json.loads(line)["entity_id"])
            except (ValueError,KeyError): pass
    session=requests.Session()
    session.headers.update({"User-Agent":os.getenv("WIKI_USER_AGENT","ece-imdb-big-data-course-project/1.0 (replace with contact email)")})
    with RAW_JSONL.open("a",encoding="utf-8") as output:
        for kind, rows in (("film",films),("person",people)):
          for row in rows:
            entity=row["tconst"] if kind=="film" else row["nconst"]
            if entity in cached_ids: continue
            name=row["primaryTitle"] if kind=="film" else row["primaryName"]
            page=_page(session, f'"{name}" film' if kind=="film" else f'"{name}" actor OR director OR film')
            rec={"entity_id":entity,"entity_type":kind,"display_name":name,"source_url":page["source_url"] if page else None,"source_title":page["page_title"] if page else None,"human_authored_text":page["text"] if page else None,"availability_note":None if page else "No matching Wikipedia page found", "retrieved_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())}
            output.write(json.dumps(rec,ensure_ascii=False)+"\n")
            output.flush()
            cached_ids.add(entity)
            time.sleep(1.0)  # polite request pacing; cached records are not requested again
    schema=T.StructType([T.StructField("entity_id",T.StringType()),T.StructField("entity_type",T.StringType()),T.StructField("display_name",T.StringType()),T.StructField("source_url",T.StringType()),T.StructField("source_title",T.StringType()),T.StructField("human_authored_text",T.StringType()),T.StructField("availability_note",T.StringType()),T.StructField("retrieved_at",T.StringType())])
    enrich=s.read.schema(schema).json(str(RAW_JSONL))
    result=(selected.join(enrich.filter("entity_type='film'").select(F.col("entity_id").alias("tconst"),"source_url","human_authored_text","availability_note"),"tconst","left")
            .join(principal_counts,"tconst","left")
            .withColumn("principal_count",F.coalesce("principal_count",F.lit(0)))
            .withColumn("principal_coverage_note",F.when(F.col("principal_count")<3,"IMDb lists fewer than three selected principal records"))
            .join(principal.select("tconst","nconst","primaryName","person_rank"),"tconst","left")
            .join(enrich.filter("entity_type='person'").select(F.col("entity_id").alias("nconst"),F.col("source_url").alias("person_source_url"),F.col("human_authored_text").alias("person_biography"),F.col("availability_note").alias("person_availability_note")),"nconst","left"))
    write_parquet(result,Path(__file__).resolve().parents[2]/"data"/"processed"/"enrichment")
    s.stop()
