"This module contains producer code for producing click conversion event."
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v.encode("utf-8")
)

def send_click_conversion(click: str) -> None:
    """send_click_conversion produces event.

    Args:
        click (str): click csv row
    """
    producer.send("clicks_conversions", click)
    producer.flush()

if __name__ == "__main__":
    click_conversion = "timestamp,user_id,ad_id,conversion_type\n2024-05-18T00:01:00Z,user1,123,signup"
    send_click_conversion(click_conversion)
