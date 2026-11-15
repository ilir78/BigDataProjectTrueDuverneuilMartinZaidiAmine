"""Forward Wikimedia's public EventStreams feed to Kafka; keep running for the test window."""
import json, os, time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[1]/".env")

URL="https://stream.wikimedia.org/v2/stream/recentchange"
def main():
    servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
    topic=os.getenv("KAFKA_TOPIC","wiki-recentchange")
    producer=KafkaProducer(bootstrap_servers=servers.split(","),value_serializer=lambda x:json.dumps(x).encode("utf-8"),acks="all",retries=5)
    headers={"User-Agent":os.getenv("WIKI_USER_AGENT","ece-imdb-big-data-course-project/1.0 (replace with contact email)")}
    while True:
        try:
            with requests.get(URL,headers=headers,stream=True,timeout=(10,90)) as response:
                response.raise_for_status()
                for line in response.iter_lines(decode_unicode=True):
                    if not line or not line.startswith("data: "): continue
                    try: event=json.loads(line[6:])
                    except json.JSONDecodeError: continue
                    producer.send(topic,event)
        except requests.RequestException as exc:
            print(f"EventStreams disconnected: {exc}; retrying in 5 seconds",flush=True)
            time.sleep(5)
if __name__=="__main__": main()
