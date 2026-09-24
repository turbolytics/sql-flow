"""Sensor drivers. Each read() returns one reading per metric.

The simulated driver stands in for real sensors on the Pi. A real driver
returns the same shape, so the publish path and the SQL never change.
"""
import random
from datetime import datetime, timezone

# (sensor, metric, unit, start, step) for a random walk per metric.
SIMULATED = [
    ("bme280", "temperature", "C", 21.0, 0.05),
    ("bme280", "humidity", "%", 45.0, 0.2),
    ("bme280", "pressure", "hPa", 1013.0, 0.1),
    ("pms5003", "pm2_5", "ug/m3", 8.0, 0.5),
    ("scd41", "co2", "ppm", 600.0, 5.0),
    ("bh1750", "lux", "lx", 300.0, 10.0),
]


class SimulatedDriver:
    def __init__(self, device_id, seed=None):
        self.device_id = device_id
        self.rng = random.Random(seed)
        self.values = {m: start for _, m, _, start, _ in SIMULATED}
        self.seq = {m: 0 for _, m, _, _, _ in SIMULATED}

    def read(self):
        ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        out = []
        for sensor, metric, unit, _, step in SIMULATED:
            self.values[metric] += self.rng.uniform(-step, step)
            self.seq[metric] += 1
            out.append({
                "device_id": self.device_id,
                "sensor": sensor,
                "metric": metric,
                "value": round(self.values[metric], 3),
                "unit": unit,
                "seq": self.seq[metric],
                "ts": ts,
            })
        return out
