"""Publishes sensor readings to MQTT at QoS 1 and records what the broker
acknowledged.

acked.csv is the ground truth for the POC's loss check. A reading appears
there only after the broker's PUBACK. A reading the broker never
acknowledged may or may not reach the sink, and the check makes no claim
about it.
"""
import argparse
import csv
import json
import os
import signal
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from drivers import SimulatedDriver


def reading_topic(r):
    return f"sensors/{r['device_id']}/{r['metric']}"


class AckLedger:
    """Maps in-flight message IDs to readings and appends acknowledged ones.

    paho can deliver a PUBACK before publish() returns the message ID, so an
    ack for an unknown ID is remembered and matched when sent() arrives.
    """

    def __init__(self, path):
        self.lock = threading.Lock()
        self.inflight = {}
        self.early = set()
        self.f = open(path, "w", newline="")
        self.w = csv.writer(self.f)
        self.w.writerow(["device_id", "metric", "seq"])
        self.count = 0

    def _write(self, row):
        self.w.writerow(row)
        self.count += 1
        if self.count % 1000 == 0:
            self.f.flush()

    def sent(self, mid, r):
        row = (r["device_id"], r["metric"], r["seq"])
        with self.lock:
            if mid in self.early:
                self.early.discard(mid)
                self._write(row)
            else:
                self.inflight[mid] = row

    def acked(self, mid):
        with self.lock:
            row = self.inflight.pop(mid, None)
            if row is None:
                self.early.add(mid)
            else:
                self._write(row)

    def pending(self):
        with self.lock:
            return len(self.inflight)

    def close(self):
        with self.lock:
            self.f.flush()
            self.f.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default=os.environ.get("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    p.add_argument("--device-id", default=os.environ.get("DEVICE_ID", "pi-1"))
    p.add_argument("--rate", type=float, default=float(os.environ.get("RATE", "100")),
                   help="readings per second, across all metrics")
    p.add_argument("--duration", type=float, default=float(os.environ.get("DURATION", "0")),
                   help="seconds to publish; 0 runs until SIGTERM")
    p.add_argument("--out", default=os.environ.get("OUT", "acked.csv"))
    a = p.parse_args()

    ledger = AckLedger(a.out)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                    client_id=f"collector-{a.device_id}", protocol=mqtt.MQTTv5)
    # paho resends an unacknowledged publish after a broker restart only if
    # the session survives the reconnect.
    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = 3600
    c.max_inflight_messages_set(1000)

    def on_publish(client, userdata, mid, reason_code, properties):
        if not reason_code.is_failure:
            ledger.acked(mid)

    c.on_publish = on_publish
    c.connect(a.host, a.port, keepalive=30, clean_start=False, properties=props)
    c.loop_start()

    driver = SimulatedDriver(device_id=a.device_id)
    interval = len(driver.values) / a.rate
    started = time.monotonic()
    next_at = started
    published = 0
    while not stop.is_set():
        if a.duration and time.monotonic() - started >= a.duration:
            break
        for r in driver.read():
            info = c.publish(reading_topic(r), json.dumps(r), qos=1)
            ledger.sent(info.mid, r)
            published += 1
        next_at += interval
        delay = next_at - time.monotonic()
        if delay > 0:
            stop.wait(delay)

    # Give in-flight publishes their PUBACKs before writing the last rows.
    deadline = time.monotonic() + 10
    while ledger.pending() and time.monotonic() < deadline:
        time.sleep(0.1)
    elapsed = time.monotonic() - started
    c.disconnect()
    c.loop_stop()
    ledger.close()
    print(json.dumps({
        "published": published,
        "acked": ledger.count,
        "unacked": ledger.pending(),
        "seconds": round(elapsed, 1),
        "publish_rate": round(published / elapsed, 1) if elapsed else 0,
    }), flush=True)


if __name__ == "__main__":
    main()
