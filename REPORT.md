# Report

# Preparation

We measured missing values for all 39 columns in the seven IMDb tables. our null_rates.csv file gives the missing count, total count, and percentage for each column.

| Table | Column | Missing rows | Total rows | Missing |
| akas | attributes | 60,011,732 | 60,328,927 | 99.474% |
| basics | endYear | 12,787,682 | 12,952,048 | 98.731% |
| names | deathYear | 15,541,545 | 15,806,985 | 98.321% |
| names | birthYear | 15,121,253 | 15,806,985 | 95.662% |
| principals | job | 82,823,599 | 102,683,165 | 80.659% |

The available audits found two people whose death year preceded their birth year, zero orphan alternate-title records, and zero orphan crew title records. Another file contains 822 person–title edges, but the original folder name is needed to identify the check, and counts for the remaining audits are unavailable.



# Methodological Explanation

Spark treats IMDb's "\N" marker and strings that are empty after trimming as missing, then divides each missing count by the table's row count. We kept missing values because some fields, such as a film's "endYear", may legitimately be empty, and filling or dropping such records would either introduce assumptions or discard usable data.

The preparation step deduplicates cleaned records by IMDb ID, casts numeric columns, and excludes impossible birth and death dates and invalid ratings from the relevant cleaned tables. Left-anti joins identify orphan references and write them to separate audit outputs for inspection.


# Exploration

The detailed CSV outputs for preparation, exploration, and the open question are in reports/tables/.

Runtime outliers : 13,479 film–genre rows.
Top three genres by year and comparison with the rest — 138 rankings across 46 years. The top three accounted for 44.30% of summed genre votes in 1980 and ranged from 38.33% in 2007 to 58.26% in 1994.
Directors :1,554 met the rating criteria.
Television series — zero matches under the rated-episode rule.
Actors in two or more languages and actors in three or more languages — both queries returned zero matches with the available original-title language data.
Early career peaks — 9,747 performers met the career and rating criteria.
Multi-talented professionals and multi-role performance — 241,907 had multiple role types and 120,910 had rated multi-role projects. Among the 59,233 with averages in both categories, multi-role projects averaged 0.058 rating points higher when each person's difference was weighted equally.



# Methodological Explanation

We used non-adult movies and a 40-minute minimum for the runtime and genre analyses. Each runtime cutoff is two standard deviations from its genre mean in genres with at least 100 films, while a median-based cutoff would be less sensitive to extreme runtimes.
We ranked genres by IMDb vote count as a measure of engagement, with each film contributing its votes to every listed genre. Ranking by film count would measure how common each genre is.

The director query requires exactly one film rated at least 8.0 and at least two other rated films averaging at most 6.0. The series query requires three seasons with at least ten rated episodes each, the worst rated episode in the last season observed among rated episodes, and the best in season one or two. Counting all listed episodes for season eligibility could produce a different result.

The language query uses the IMDb original-title language field and a rating threshold of 7.0. The career query uses at least 15 rated movies over at least 20 years and checks whether the first highest-rated movie falls within the first 33% of that span.

We joined acting, writing, and directing credits by person and title to identify people with at least two role types. We compared their mean ratings for single-role and multi-role films as a descriptive measure, since the sets of films can differ.

# Enrichment


The available output contains 293 rows across 100 year–genre–film selections, covering 77 distinct films, all from 1980. All 77 have a nonempty Wikipedia extract, 212 of 216 distinct film–principal pairs have text (98.15%), and 61 of 77 films have text for at least three principals (79.22%).

These rates measure text availability in the partial run. A spot check found that for example "Everything Happens to Me" links to the 1938 film of the same name and Bud Spencer links to the Terence Hill article, so verified coverage for 1980–2025 remains unmeasured.



# Methodological Explanation

Spark ranks up to ten feature-length films per genre and release year by votes and selects up to three acting, directing, or writing credits per film. It enforces the schema on the collected data and joins by IMDb ID, retaining source URLs and availability notes.

We used English Wikipedia to keep collection and provenance consistent, with caching, a pause between new entity requests, and "Retry-After" handling. Taking the first result and its introduction reduces collection effort, but checking entity identity and retrieving dedicated plot sections would improve accuracy and synopsis coverage at the cost of additional work.


# Streaming

The full run on November 13, 2026 lasted 45 min 15 sec and processed 150 events in 273 micro-batches, of which 114 were nonempty. The first 30 minutes, 21:31:11.856–22:01:11.856 CET, contained 107 processed events across 181 batches, including 78 nonempty batches.

Counts come from the stream_metrics.jsonl file after filtering and deduplication. The run notes report 258 Parquet part files in the streaming output.




# Methodological Explanation

The producer forwards Wikimedia RecentChanges to Kafka, and Spark filters English Wikipedia main-namespace edits by film and television keywords in page titles. This broad filter captured 107 potentially relevant events in 30 minutes, while matching IMDb or Wikipedia page IDs could improve precision and would require a maintained mapping.

Spark uses a ten-minute watermark based on the Wikimedia event timestamp and deduplicates by event ID and event time. A longer watermark would keep more state and allow more delayed events, while a shorter one could drop more late arrivals.


# Open Question

Which genres gained the largest share of cumulative IMDb votes between the 1980 and 2025 film release cohorts?

Action gained 10.68 percentage points (2.92% to 13.60%), followed by Thriller (+5.79) and Crime (+4.02). The open_question_genre_trends.csv file covers the 23 genres present in both cohorts.




# Methodological Explanation

We joined IMDb titles and ratings, then divided each genre's votes by all genre-assigned votes in the same release cohort. This lets us compare the mix of votes across cohorts of different sizes using IMDb data alone, though older films had more time to accumulate votes.

Our approach assigns the full vote count to every listed genre. Splitting votes across genres would count each vote once, while giving each genre less weight for films with multiple labels.
