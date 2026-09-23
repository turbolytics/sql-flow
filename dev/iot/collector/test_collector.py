import csv
import json

from collector import AckLedger, reading_topic
from drivers import SimulatedDriver


def test_simulated_driver_emits_narrow_readings_with_gapless_seq():
    d = SimulatedDriver(device_id="pi-1", seed=7)
    readings = [r for _ in range(3) for r in d.read()]
    by_metric = {}
    for r in readings:
        assert set(r) == {"device_id", "sensor", "metric", "value", "unit", "seq", "ts"}
        by_metric.setdefault(r["metric"], []).append(r["seq"])
    assert len(by_metric) >= 5
    for seqs in by_metric.values():
        assert seqs == list(range(1, len(seqs) + 1))


def test_reading_topic():
    assert reading_topic({"device_id": "pi-1", "metric": "co2"}) == "sensors/pi-1/co2"


def test_ack_ledger_records_only_acknowledged(tmp_path):
    out = tmp_path / "acked.csv"
    ledger = AckLedger(str(out))
    ledger.sent(11, {"device_id": "pi-1", "metric": "co2", "seq": 1})
    ledger.sent(12, {"device_id": "pi-1", "metric": "co2", "seq": 2})
    ledger.acked(12)
    ledger.close()
    rows = list(csv.DictReader(out.open()))
    assert rows == [{"device_id": "pi-1", "metric": "co2", "seq": "2"}]
    assert ledger.pending() == 1


# paho can deliver a PUBACK before publish() returns the message ID.
def test_ack_ledger_handles_an_ack_before_sent(tmp_path):
    out = tmp_path / "acked.csv"
    ledger = AckLedger(str(out))
    ledger.acked(5)
    ledger.sent(5, {"device_id": "pi-1", "metric": "lux", "seq": 9})
    ledger.close()
    rows = list(csv.DictReader(out.open()))
    assert rows == [{"device_id": "pi-1", "metric": "lux", "seq": "9"}]
    assert ledger.pending() == 0


def test_readings_serialize_as_json():
    d = SimulatedDriver(device_id="pi-1", seed=1)
    r = d.read()[0]
    assert json.loads(json.dumps(r))["device_id"] == "pi-1"
