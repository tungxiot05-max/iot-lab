"""Thiết bị giả lập: publish JSON giống firmware ESP32 để test pipeline khi không chạy Wokwi.

Ví dụ:
  python tools/fake_device.py --host localhost --interval 1 --faults
"""
from __future__ import annotations

import argparse
import json
import math
import random
import time

import paho.mqtt.client as mqtt


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--site", default="ptit")
    ap.add_argument("--location", default="lab")
    ap.add_argument("--device", default="esp32-tung-01")
    ap.add_argument("--interval", type=float, default=2.0, help="giây giữa hai gói")
    ap.add_argument("--count", type=int, default=0, help="số gói (0 = chạy mãi)")
    ap.add_argument("--faults", action="store_true",
                    help="chèn lỗi: spike, mất gói, gói trùng, JSON hỏng, NaN")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    topic = f"iot/{args.site}/{args.location}/{args.device}/telemetry"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"fake-{args.device}-{rng.randint(0, 9999)}")
    c.connect(args.host, args.port, keepalive=30)
    c.loop_start()
    print(f"Publishing to {args.host}:{args.port} topic {topic}")

    seq, t0 = 0, time.time()
    temp, hum = 27.0, 65.0
    try:
        while args.count == 0 or seq < args.count:
            seq += 1
            elapsed = time.time() - t0
            temp += rng.gauss(0, 0.05) + 0.02 * math.sin(elapsed / 60)
            hum = min(100, max(0, hum + rng.gauss(0, 0.1)))
            light = max(0.0, 300 + 200 * math.sin(elapsed / 120) + rng.gauss(0, 5))
            msg = {
                "device_id": args.device, "location": args.location, "seq": seq,
                "ts": time.time_ns() // 1_000_000,
                "temperature": round(temp, 1), "humidity": round(hum, 1),
                "light": round(light, 1), "light_raw": int(min(4095, light * 4)),
                "motion": int(rng.random() < 0.1), "rssi": rng.randint(-75, -55),
                "uptime_s": int(elapsed),
            }
            payload = json.dumps(msg)

            if args.faults:
                r = rng.random()
                if r < 0.03:
                    print(f"[fault] drop seq={seq}")
                    time.sleep(args.interval)
                    continue
                if r < 0.06:
                    msg["temperature"] = round(temp + rng.choice([-1, 1]) * 30, 1)
                    payload = json.dumps(msg)
                    print(f"[fault] spike seq={seq} temp={msg['temperature']}")
                elif r < 0.08:
                    payload = payload[:-5]
                    print(f"[fault] broken JSON seq={seq}")
                elif r < 0.10:
                    payload = payload.replace(f'"humidity": {msg["humidity"]}', '"humidity": NaN')
                    print(f"[fault] NaN seq={seq}")
                elif r < 0.12:
                    c.publish(topic, payload, qos=1)
                    print(f"[fault] duplicate seq={seq}")

            c.publish(topic, payload, qos=1)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        time.sleep(0.5)
        c.loop_stop()
        c.disconnect()


if __name__ == "__main__":
    main()
