import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingest.validator import SeqTracker, ValidationError, validate  # noqa: E402

NOW = 1_760_750_000_000
TOPIC = "iot/ptit/lab/esp32-tung-01/telemetry"


def payload(**over):
    base = {"device_id": "esp32-tung-01", "location": "lab", "seq": 10, "ts": NOW - 300,
            "temperature": 25.3, "humidity": 60.1, "light": 320.5, "light_raw": 1200,
            "motion": 0, "rssi": -60, "uptime_s": 120}
    base.update(over)
    return json.dumps(base)


def test_valid_payload():
    rec = validate(payload(), TOPIC, NOW)
    assert rec["temperature"] == 25.3 and rec["seq"] == 10 and rec["site"] == "ptit"
    assert isinstance(rec["humidity"], float)


def test_int_temperature_is_cast_to_float():
    rec = validate(payload(temperature=25), TOPIC, NOW)
    assert isinstance(rec["temperature"], float)


@pytest.mark.parametrize("raw,reason", [
    ("{not json", "JSON"),
    ("[1,2]", "đối tượng"),
    (payload(humidity=None), "humidity"),
    (payload(temperature="25"), "Sai kiểu"),
    (payload(motion=True), "Sai kiểu"),
    (payload(humidity=120), "ngoài dải"),
    (payload(temperature=-50), "ngoài dải"),
    (payload(ts=12345), "NTP"),
    (payload(ts=NOW + 120_000), "tương lai"),
    (payload(device_id="other"), "không khớp"),
    ('{"device_id":"esp32-tung-01","seq":1,"ts":%d,"temperature":NaN,"humidity":50}' % NOW, "NaN"),
])
def test_invalid_payloads(raw, reason):
    with pytest.raises(ValidationError, match=reason):
        validate(raw, TOPIC, NOW)


def test_seq_tracker():
    t = SeqTracker()
    assert t.check("d", 100, 200).status == "first"
    assert t.check("d", 101, 202).status == "ok"
    assert t.check("d", 101, 202).status == "duplicate"
    r = t.check("d", 104, 208)
    assert r.status == "gap" and r.lost == 2
    assert t.check("d", 103, 206).status == "out_of_order"   # gói đến muộn
    assert t.check("d", 105, 210).status == "ok"
    assert t.check("d", 1, 0).status == "reboot"             # thiết bị reset
    assert t.check("d", 2, 2).status == "ok"
