from pyspark.sql import functions as F, Window
from .common import RESULTS, read_table, spark, write_csv

def _write_csv_once(df,path):
    if (path/"_SUCCESS").exists():
        print(f"Already complete, keeping {path}")
    else:
        write_csv(df,path)

def run():
    s = spark("IMDb exploration")
    basics = read_table(s, "basics").filter((F.col("titleType") == "movie") & (F.col("isAdult") == "0"))
    ratings = read_table(s, "ratings").withColumn("rating", F.col("averageRating").cast("double")).withColumn("votes", F.col("numVotes").cast("long"))
    names, crew = read_table(s, "names"), read_table(s, "crew")
    principals, akas = read_table(s, "principals"), read_table(s, "akas")
    out = RESULTS / "exploration"
    films = basics.join(ratings, "tconst", "left").withColumn("year", F.col("startYear").cast("int")).withColumn("runtime", F.col("runtimeMinutes").cast("int")).withColumn("genres_arr", F.split("genres", ","))
    genre_films = films.filter(F.col("year").isNotNull() & (F.col("runtime")>=40)).withColumn("genre", F.explode("genres_arr"))
    avgs = genre_films.groupBy("genre").agg(F.avg("runtime").alias("genre_avg"), F.stddev("runtime").alias("genre_sd"), F.count("*").alias("genre_n"))
    # Robustly define notably different as >=2 genre-specific standard deviations, with >=100 films.
    outliers=genre_films.join(avgs, "genre").filter((F.col("genre_n") >= 100) & (F.abs(F.col("runtime")-F.col("genre_avg")) >= 2*F.col("genre_sd"))).select("tconst","primaryTitle","year","genre","runtime","genre_avg","genre_sd").orderBy(F.desc(F.abs(F.col("runtime")-F.col("genre_avg"))))
    _write_csv_once(outliers, out / "runtime_outliers.csv")

    yearly = genre_films.filter(F.col("year").between(1980,2025)).groupBy("year","genre").agg(F.sum(F.coalesce("votes",F.lit(0))).alias("genre_votes"), F.countDistinct("tconst").alias("film_count"))
    w = Window.partitionBy("year").orderBy(F.desc("genre_votes"), F.asc("genre"))
    ranked = yearly.withColumn("rank", F.row_number().over(w))
    _write_csv_once(ranked.filter("rank <= 3").orderBy("year","rank"), out / "top_genres_by_year.csv")
    comparison = ranked.groupBy("year").agg(F.sum(F.when(F.col("rank") <= 3,F.col("genre_votes")).otherwise(0)).alias("top3_votes"), F.sum(F.when(F.col("rank") > 3,F.col("genre_votes")).otherwise(0)).alias("other_votes"), F.sum("genre_votes").alias("all_votes"))
    comparison = comparison.withColumn("top3_vote_share", F.col("top3_votes")/F.col("all_votes"))
    _write_csv_once(comparison.orderBy("year"), out / "top_genres_vs_rest.csv")

    director_edges = crew.select("tconst",F.explode(F.split("directors", ",")).alias("nconst"))
    director_titles = director_edges.join(films.select("tconst","primaryTitle","year").join(ratings.select("tconst","rating"),"tconst"),"tconst").groupBy("nconst").agg(F.sum(F.when(F.col("rating")>=8,1).otherwise(0)).alias("high_count"), F.sum(F.when(F.col("rating")<8,1).otherwise(0)).alias("other_count"), F.avg(F.when(F.col("rating")<8,F.col("rating"))).alias("other_avg"))
    _write_csv_once(director_titles.join(names,"nconst").filter((F.col("high_count")==1)&(F.col("other_count")>=2)&(F.col("other_avg")<=6)).select("nconst","primaryName","high_count","other_count","other_avg").orderBy("primaryName"), out / "directors_rating_pattern.csv")

    eps = read_table(s,"episode").withColumn("season",F.col("seasonNumber").cast("int")).join(ratings.select("tconst","rating"),"tconst")
    seasons = eps.groupBy("parentTconst","season").agg(F.countDistinct("tconst").alias("episodes"))
    eligible = seasons.groupBy("parentTconst").agg(F.countDistinct(F.when(F.col("episodes")>=10,"season")).alias("qualifying_seasons"), F.max("season").alias("last_season")) .filter(F.col("qualifying_seasons")>=3).select("parentTconst","last_season")
    ranks = eps.groupBy("parentTconst","season").agg(F.min("rating").alias("season_min"), F.max("rating").alias("season_max"))
    extrema = eps.groupBy("parentTconst").agg(F.min("rating").alias("worst"), F.max("rating").alias("best"))
    worst_season = eps.join(extrema, ["parentTconst"]).filter(F.col("rating")==F.col("worst")).groupBy("parentTconst").agg(F.max("season").alias("worst_season"))
    best_season = eps.join(extrema,["parentTconst"]).filter(F.col("rating")==F.col("best")).groupBy("parentTconst").agg(F.min("season").alias("best_season"))
    series = eligible.join(worst_season,"parentTconst").join(best_season,"parentTconst").filter((F.col("worst_season")==F.col("last_season"))&(F.col("best_season")<=2))
    _write_csv_once(series.join(read_table(s,"basics").select(F.col("tconst").alias("parentTconst"),"primaryTitle"),"parentTconst").orderBy("primaryTitle"), out / "series_extreme_episodes.csv")

    # Original-language evidence comes from IMDb's original-title AKA flag; undefined languages are omitted.
    languages = akas.filter(F.col("isOriginalTitle")=="1").select(F.col("titleId").alias("tconst"),F.col("language").alias("language")).filter(F.col("language").isNotNull()).dropDuplicates(["tconst"])
    actor_films = principals.filter(F.col("category").isin("actor","actress")).join(films.select("tconst").distinct(),"tconst").join(ratings.filter(F.col("rating")>=7.0).select("tconst","rating"),"tconst").join(languages,"tconst").join(names.select("nconst","primaryName"),"nconst")
    actor_lang = actor_films.groupBy("nconst","primaryName").agg(F.countDistinct("language").alias("language_count"),F.collect_set("language").alias("languages"),F.countDistinct("tconst").alias("successful_films"))
    _write_csv_once(actor_lang.filter(F.col("language_count")>=2).orderBy(F.desc("language_count"),"primaryName"), out / "actors_multilingual_success.csv")
    _write_csv_once(actor_lang.filter(F.col("language_count")>=3).orderBy(F.desc("language_count"),"primaryName"), out / "actors_3plus_languages.csv")

    career_movies=(principals.filter(F.col("category").isin("actor","actress")).select("nconst","tconst").dropDuplicates()
                   .join(films.select("tconst",F.col("year").alias("film_year")),"tconst").join(ratings.select("tconst","rating"),"tconst")
                   .join(names.select("nconst","primaryName"),"nconst"))
    career_stats=career_movies.groupBy("nconst","primaryName").agg(F.min("film_year").alias("first_year"),F.max("film_year").alias("last_year"),F.countDistinct("tconst").alias("film_count"),F.max("rating").alias("best_rating"))
    best=career_movies.join(career_stats.select("nconst","best_rating"),"nconst").filter(F.col("rating")==F.col("best_rating")).groupBy("nconst").agg(F.min("film_year").alias("best_year"))
    careers = career_stats.join(best,"nconst").filter((F.col("last_year")-F.col("first_year")>=20)&(F.col("film_count")>=15))
    careers = careers.withColumn("career_fraction_at_peak",(F.col("best_year")-F.col("first_year"))/(F.col("last_year")-F.col("first_year")))
    _write_csv_once(careers.filter(F.col("career_fraction_at_peak")<=0.33).orderBy("career_fraction_at_peak"), out / "actors_early_peak_careers.csv")

    actor_edges = principals.filter(F.col("category").isin("actor","actress")).select("tconst","nconst")
    director_edges = crew.select("tconst",F.explode(F.split("directors", ",")).alias("nconst"))
    writer_edges = crew.select("tconst",F.explode(F.split("writers", ",")).alias("nconst"))
    roles = actor_edges.withColumn("role",F.lit("actor")).unionByName(director_edges.withColumn("role",F.lit("director"))).unionByName(writer_edges.withColumn("role",F.lit("writer"))).dropDuplicates(["tconst","nconst","role"])
    roles = roles.join(films.select("tconst").distinct(),"tconst")
    multi = roles.groupBy("nconst").agg(F.countDistinct("role").alias("role_count"),F.collect_set("role").alias("roles"))
    _write_csv_once(multi.filter(F.col("role_count")>=2).join(names,"nconst").select("nconst","primaryName","roles","role_count").orderBy(F.desc("role_count"),"primaryName"), out / "multitalented_professionals.csv")
    same_project = roles.groupBy("nconst","tconst").agg(F.countDistinct("role").alias("roles_on_title"),F.collect_set("role").alias("roles" )).filter(F.col("roles_on_title")>=2).join(ratings,"tconst").groupBy("nconst").agg(F.count("*").alias("multi_role_titles"),F.avg(F.col("averageRating").cast("double")).alias("multi_role_avg_rating"))
    solo = roles.groupBy("nconst","tconst").agg(F.countDistinct("role").alias("role_count")).filter(F.col("role_count")==1).join(ratings,"tconst").groupBy("nconst").agg(F.avg(F.col("averageRating").cast("double")).alias("single_role_avg_rating"))
    _write_csv_once(same_project.join(solo,"nconst","left").join(names,"nconst").orderBy(F.desc("multi_role_titles")), out / "multi_role_project_performance.csv")

    # Open question: year-normalized genre vote share trend; weighted by votes as IMDb popularity proxy.
    genre_year = yearly.withColumn("vote_share",F.col("genre_votes")/F.sum("genre_votes").over(Window.partitionBy("year")))
    base = genre_year.filter(F.col("year")==1980).select("genre",F.col("vote_share").alias("share_1980"))
    end = genre_year.filter(F.col("year")==2025).select("genre",F.col("vote_share").alias("share_2025"))
    _write_csv_once(base.join(end,"genre").withColumn("share_change",F.col("share_2025")-F.col("share_1980")).orderBy(F.desc("share_change")), out / "open_question_genre_trends.csv")
    s.stop()
