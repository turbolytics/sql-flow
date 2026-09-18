import gzip, json
from confluent_kafka import Producer

COLUMNS = ["trip_id", "pickup_datetime", "dropoff_datetime", "pickup_longitude",
           "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "passenger_count",
           "trip_distance", "fare_amount", "extra", "tip_amount", "tolls_amount",
           "total_amount", "payment_type", "pickup_ntaname", "dropoff_ntaname"]
INTS = {"trip_id", "passenger_count"}
FLOATS = {"pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
          "trip_distance", "fare_amount", "extra", "tip_amount", "tolls_amount", "total_amount"}

producer = Producer({"bootstrap.servers": "localhost:9092"})

def send(topic, value):
    # The producer's queue holds 100,000 messages by default, and this file
    # has a million rows; poll() drains it as it fills.
    while True:
        try:
            producer.produce(topic, value)
            return
        except BufferError:
            producer.poll(0.5)

with gzip.open("trips_0.gz", "rb") as f:
    header = f.readline().decode().rstrip("\n").split("\t")
    index = {name: i for i, name in enumerate(header)}
    for raw in f:
        # Split on the raw bytes and decode only the columns being published.
        # The file carries columns this example does not use that are not
        # valid UTF-8, and decoding the whole line would have to corrupt them.
        parts = raw.rstrip(b"\n").split(b"\t")
        row = {}
        for col in COLUMNS:
            value = parts[index[col]].decode()
            if value == "":
                row[col] = None
            elif col in INTS:
                row[col] = int(float(value))
            elif col in FLOATS:
                row[col] = float(value)
            else:
                row[col] = value
        send("nyc-taxi-trips", json.dumps(row))
producer.flush()
