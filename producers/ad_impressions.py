"This module contains producer code for producing ad impressions event."
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_ad_impression(ad_impression: dict[str,any]) -> None:
    """send_ad_impression produces event.

    Args:
        ad_impression dict[str,any]: event json
    """
    producer.send("ad_impressions", ad_impression)
    producer.flush()


if __name__ == "__main__":
    ad_impression = {
        "ad_id": "123",
        "user_id": "user1",
        "timestamp": "2024-05-18T00:00:00Z",
        "website": "example.com"
    }

    send_ad_impression(ad_impression)
