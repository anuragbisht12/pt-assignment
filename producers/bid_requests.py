"This module contains producer code for producing bid request event."
import avro.schema
import avro.io
from kafka import KafkaProducer
import io

producer = KafkaProducer(bootstrap_servers="localhost:9092")

schema = avro.schema.Parse(open("bid_request.avsc", "r").read())

def send_bid_request(raw_bytes: bytes)-> None:
    """send_bid_request produces bid request event.

    Args:
        bid_request (bytes): event
    """
    producer.send("bid_requests", raw_bytes)
    producer.flush()


if __name__ == "__main__":
    bid_request = {
        "user_id": "user1",
        "auction_id": "auction123",
        "targeting_criteria": "criteria"
    }
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(bid_request, encoder)
    raw_bytes = bytes_writer.getvalue()
    send_bid_request(raw_bytes)
