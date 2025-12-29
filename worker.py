"""
Lightweight worker prototype consuming dispatch queue and emitting result messages.
"""
import json
import os
import uuid
import hashlib
import logging
from datetime import datetime, timedelta

import pika

logging.basicConfig(level=logging.INFO)

AMQP_URI = os.getenv("SAAI_AMQP_URI", "amqp://guest:guest@localhost:5672/")
EXCHANGE = os.getenv("SAAI_DISPATCH_EXCHANGE", "sys.core")
DISPATCH_QUEUE = os.getenv("SAAI_DISPATCH_QUEUE", "task_dispatch_q")
RESULT_ROUTING = os.getenv("SAAI_RESULT_ROUTING", "sys.core.task.result")


class Worker:
    def __init__(self):
        params = pika.URLParameters(AMQP_URI)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
        self.channel.queue_declare(queue=DISPATCH_QUEUE, durable=True)
        self.channel.queue_bind(queue=DISPATCH_QUEUE, exchange=EXCHANGE, routing_key="sys.core.task.dispatch")

    def start(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(DISPATCH_QUEUE, self.process_message)
        logging.info("Worker listening on %s", DISPATCH_QUEUE)
        self.channel.start_consuming()

    def process_message(self, ch, method, properties, body):
        message = json.loads(body.decode())
        logging.info("Received task %s", message.get("task_id"))
        result = self.execute(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self.publish_result(result)

    def execute(self, message):
        content = message.get("instruction", "")
        sha = hashlib.sha256(content.encode()).hexdigest()
        return {
            "status": "OK",
            "project_id": message.get("project_id"),
            "run_id": message.get("run_id"),
            "task_id": message.get("task_id"),
            "attempt_id": message.get("attempt_id"),
            "correlation_id": message.get("correlation_id"),
            "idempotency_key": message.get("idempotency_key"),
            "claim_token": message.get("claim_token"),
            "deadline_ts": (datetime.utcnow() + timedelta(seconds=5)).isoformat() + "Z",
            "artifacts": [
                {
                    "artifact_type": "LOG",
                    "content_text": f"Executed instruction: {content}",
                    "sha256": sha,
                }
            ],
            "diff_summary": "noop",
            "usage_stats": {"tokens": len(content.split()), "cost": 0}
        }

    def publish_result(self, payload):
        self.channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=RESULT_ROUTING,
            body=json.dumps(payload).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logging.info("Published result for task %s", payload.get("task_id"))


def main():
    worker = Worker()
    worker.start()


if __name__ == "__main__":
    main()
