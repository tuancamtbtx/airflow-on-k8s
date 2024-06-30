# -*- coding: UTF-8 -*-
from __future__ import print_function
import logging

import time
from typing import Any
from confluent_kafka import Producer
from airflow.hooks.base import BaseHook
from urllib.parse import unquote


class KafkaProducerHook(BaseHook):
    def __init__(self, kafka_conn_id):
        super().__init__()
        connection = self.get_connection(kafka_conn_id)
        borkers = self._parse_brokers(connection.get_uri())
        self.kafka_connector = KafkaConnector(borkers)

    def _parse_brokers(self, uri: str):
        if uri.startswith('http://'):
            uri = uri.replace('http://', "", 1)
        if uri.startswith('https://'):
            uri = uri.replace('https://', "", 1)

        return unquote(uri)

    def produce(self, topic: str, key: str, value: Any):
        if self.kafka_connector.producer is None:
            self.kafka_connector.set_producer()
        self.kafka_connector.produce(topic, key, value)


class KafkaConnector:

    producer = None

    def __init__(self, broker):
        self.broker = broker
        self.LOGGER = logging.getLogger(__name__)

    def set_producer(self):
        self._set_producer()

    def _set_producer(self):
        self.producer = Producer({"bootstrap.servers": self.broker})
        return self

    def produce(self, topic, key, value, callback=None):
        # Trigger any available delivery report callbacks from previous produce() calls
        self.producer.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        if not callback:
            callback = self.delivery_report
        self.producer.produce(topic, key=key, value=value, callback=callback)
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        self.producer.flush()

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
        if err is not None:
            raise Exception("Message delivery failed: {}".format(err))
        else:
            self.LOGGER.info(
                "Message delivered to {} [{}]".format(msg.topic(), msg.partition())
            )
