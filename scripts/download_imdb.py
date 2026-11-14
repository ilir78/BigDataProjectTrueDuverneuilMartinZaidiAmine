"""Download IMDb's official non-commercial TSV dumps to data/raw."""
from pathlib import Path
import requests

ROOT=Path(__file__).resolve().parents[1]
FILES=("title.akas.tsv.gz","title.basics.tsv.gz","title.crew.tsv.gz","title.episode.tsv.gz","title.principals.tsv.gz","title.ratings.tsv.gz","name.basics.tsv.gz")
def main():
    target=ROOT/"data"/"raw"
    target.mkdir(parents=True,exist_ok=True)
    for name in FILES:
        path=target/name
        if path.exists():
            print(f"Already present: {name}")
            continue
        url="https://datasets.imdbws.com/"+name
        print(f"Downloading {name} (IMDb personal/non-commercial terms apply)",flush=True)
        with requests.get(url,stream=True,timeout=(30,300),headers={"User-Agent":"ece-imdb-big-data-course-project/1.0"}) as response:
            response.raise_for_status()
            with path.open("wb") as out:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk: out.write(chunk)
if __name__=="__main__": main()
