"""Synthetic e-commerce clickstream event generator.

Produces realistic user-behavior events to a Kafka (Redpanda) topic so the
streaming pipeline has something to chew on. Events follow a plausible
shopping funnel -- page_view -> product_view -> add_to_cart -> purchase --
with weighted probabilities, so downstream aggregates look like a real
storefront instead of uniform noise.

A small, configurable fraction of events are intentionally malformed
(null event_type, bad timestamp, duplicate event_id, negative price) to
exercise the pipeline's data-quality checks. See --bad-record-rate.

Usage:
    python src/event_generator.py --rate 50 --duration 120
    python src/event_generator.py --rate 200 --duration 0   # run until Ctrl+C
"""

import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Catalog & behavior model
# ---------------------------------------------------------------------------
CATEGORIES = {
    "electronics": [("prod_e101", 899.99), ("prod_e102", 349.50), ("prod_e103", 129.99)],
    "fashion": [("prod_f201", 79.95), ("prod_f202", 149.00), ("prod_f203", 39.99)],
    "home": [("prod_h301", 219.00), ("prod_h302", 59.99), ("prod_h303", 89.50)],
    "sports": [("prod_s401", 119.99), ("prod_s402", 45.00), ("prod_s403", 199.99)],
    "books": [("prod_b501", 24.99), ("prod_b502", 18.50), ("prod_b503", 32.00)],
}

EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "purchase", "search"]

# Funnel transition probabilities: given the previous event in a session,
# what comes next? This produces believable drop-off (many views, few buys).
FUNNEL = {
    "page_view": [("product_view", 0.45), ("search", 0.20), ("page_view", 0.35)],
    "search": [("product_view", 0.55), ("page_view", 0.30), ("search", 0.15)],
    "product_view": [("product_view", 0.40), ("add_to_cart", 0.18), ("page_view", 0.42)],
    "add_to_cart": [("purchase", 0.35), ("product_view", 0.35), ("page_view", 0.30)],
    "purchase": [("page_view", 0.70), ("product_view", 0.30)],
}

DEVICES = [("mobile", 0.55), ("desktop", 0.35), ("tablet", 0.10)]
COUNTRIES = [("US", 0.60), ("CA", 0.10), ("GB", 0.10), ("DE", 0.08), ("IN", 0.12)]


def _weighted_choice(weighted):
    """Pick a value from a [(value, weight), ...] list."""
    values, weights = zip(*weighted)
    return random.choices(values, weights=weights, k=1)[0]


class ClickstreamGenerator:
    """Stateful generator: tracks sessions so event sequences look real."""

    def __init__(self, num_users=500, bad_record_rate=0.03):
        self.user_ids = [f"user_{i:05d}" for i in range(1, num_users + 1)]
        # session_id -> last event_type, so funnels continue across events
        self.sessions = {}
        self.bad_record_rate = bad_record_rate
        # keep recent event_ids around so we can emit realistic duplicates
        self.recent_event_ids = []

    def _new_session(self, user_id):
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = ("page_view", user_id)
        return session_id, "page_view"

    def _next_event_type(self, session_id, user_id):
        if session_id not in self.sessions:
            return self._new_session(user_id)
        last_type, _ = self.sessions[session_id]
        # sessions decay: 15% chance the session ends and a new one starts
        if random.random() < 0.15:
            return self._new_session(user_id)
        next_type = _weighted_choice(FUNNEL[last_type])
        self.sessions[session_id] = (next_type, user_id)
        return session_id, next_type

    def _corrupt(self, event):
        """Apply one realistic data-quality fault to a copy of the event."""
        event = dict(event)
        fault = random.choice(
            ["null_event_type", "bad_timestamp", "duplicate_id", "negative_price"]
        )
        if fault == "null_event_type":
            event["event_type"] = None
        elif fault == "bad_timestamp":
            event["event_time"] = "not-a-timestamp"
        elif fault == "duplicate_id" and self.recent_event_ids:
            event["event_id"] = random.choice(self.recent_event_ids)
        elif fault == "negative_price" and event["price"] is not None:
            event["price"] = -abs(event["price"])
        else:
            event["event_type"] = None  # fallback fault
        return event

    def build_event(self):
        """Build one event dict (no I/O) -- easy to test, easy to reuse."""
        user_id = random.choice(self.user_ids)
        # 70% chance of continuing an existing session for this user
        user_sessions = [s for s, (_, u) in self.sessions.items() if u == user_id]
        if user_sessions and random.random() < 0.70:
            session_id = random.choice(user_sessions)
            session_id, event_type = self._next_event_type(session_id, user_id)
        else:
            session_id, event_type = self._new_session(user_id)

        category = random.choice(list(CATEGORIES.keys()))
        product_id, price = random.choice(CATEGORIES[category])

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "product_id": product_id if event_type in ("product_view", "add_to_cart", "purchase") else None,
            "category": category,
            "price": price if event_type in ("add_to_cart", "purchase") else None,
            "device": _weighted_choice(DEVICES),
            "country": _weighted_choice(COUNTRIES),
            # ISO-8601 UTC with millis; Spark parses with yyyy-MM-dd'T'HH:mm:ss.SSSX
            "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        }

        self.recent_event_ids.append(event["event_id"])
        self.recent_event_ids = self.recent_event_ids[-1000:]

        if random.random() < self.bad_record_rate:
            event = self._corrupt(event)
        return event


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed for {msg.key()}: {err}", file=sys.stderr)


def parse_args():
    p = argparse.ArgumentParser(description="Generate clickstream events to Kafka.")
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="clickstream-events")
    p.add_argument("--rate", type=float, default=50, help="events per second")
    p.add_argument(
        "--duration",
        type=int,
        default=120,
        help="seconds to run; 0 means run until interrupted",
    )
    p.add_argument(
        "--bad-record-rate",
        type=float,
        default=0.03,
        help="fraction of events intentionally malformed (0.0-1.0)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "compression.type": "lz4",
        }
    )
    generator = ClickstreamGenerator(bad_record_rate=args.bad_record_rate)

    interval = 1.0 / args.rate if args.rate > 0 else 0
    deadline = time.time() + args.duration if args.duration > 0 else float("inf")
    sent = 0
    started = time.time()

    print(
        f"producing to topic '{args.topic}' at ~{args.rate}/s "
        f"(bad-record-rate={args.bad_record_rate}) -- Ctrl+C to stop"
    )
    try:
        while running and time.time() < deadline:
            event = generator.build_event()
            producer.produce(
                args.topic,
                key=event["event_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            sent += 1
            if interval:
                time.sleep(interval)
    finally:
        producer.flush(10)

    elapsed = time.time() - started
    print(f"done: produced {sent} events in {elapsed:.1f}s ({sent / elapsed:.1f}/s)")


if __name__ == "__main__":
    main()
