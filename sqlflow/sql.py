import os
import sys
from datetime import datetime, timezone
import socket

import duckdb
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from sqlflow.outputs import ConsoleWriter, Writer, KafkaWriter


class SQLFlow:

    def __init__(self, conf, consumer, output: Writer):
        self.conf = conf
        self.consumer = consumer
        self.output = output

    def consume_loop(self):
        try:
            self.consumer.subscribe(self.conf.pipeline.input.topics)
            self._consume_loop()
        finally:
            self.consumer.close()

    def _consume_loop(self):
        num_messages = 0
        start_dt = datetime.now(timezone.utc)
        total_messages = 0

        batch_file = os.path.join(
            self.conf.sql_results_cache_dir,
            'consumer_batch.json',
        )
        f = open(batch_file, 'w+')

        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            total_messages += 1
            if msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        '%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset()),
                        )
                elif msg.error():
                    raise KafkaException(msg.error())
                continue

            f.write(msg.value().decode())
            f.write('\n')
            num_messages += 1

            if total_messages % 10000 == 0:
                now = datetime.now(timezone.utc)
                diff = (now - start_dt)
                print(total_messages // diff.total_seconds(), ': reqs / second')

            if num_messages == self.conf.pipeline.input.batch_size:
                f.flush()
                f.close()

                # apply the pipeline
                b = InferredBatch(self.conf)
                res = b.invoke(batch_file)
                for l in res:
                    self.output.write(l)

                # Only commit after all messages in batch are processed
                self.output.flush()
                self.consumer.commit(asynchronous=False)
                # reset the file state
                f = open(batch_file, 'w+')
                f.truncate()
                f.seek(0)
                num_messages = 0


class InferredBatch:
    def __init__(self, conf):
        self.conf = conf

    def invoke(self, batch_file):
        try:
            for l in self._invoke(batch_file):
                yield l
        finally:
            duckdb.sql('DROP TABLE IF EXISTS batch')

    def _invoke(self, batch_file):
        duckdb.sql(
            'CREATE TABLE batch AS SELECT * FROM read_json_auto(\'{}\')'.format(
                batch_file
            ),
        )

        out_file = os.path.join(
            self.conf.sql_results_cache_dir,
            'out.json',
        )

        duckdb.sql(
            "COPY ({}) TO '{}'".format(
                self.conf.pipeline.sql,
                out_file,
            )
        )

        with open(out_file, 'r') as f:
            for l in f:
                yield l.strip()


def new_sqlflow_from_conf(conf) -> SQLFlow:
    kconf = {
        'bootstrap.servers': ','.join(conf.kafka.brokers),
        'group.id': conf.kafka.group_id,
        'auto.offset.reset': conf.kafka.auto_offset_reset,
        'enable.auto.commit': False,
    }

    consumer = Consumer(kconf)

    output = ConsoleWriter()
    if conf.pipeline.output.type == 'kafka':
        producer = Producer({
            'bootstrap.servers': ','.join(conf.kafka.brokers),
            'client.id': socket.gethostname(),
        })
        output = KafkaWriter(
            topic=conf.pipeline.output.topic,
            producer=producer,
        )

    sflow = SQLFlow(
        conf=conf,
        consumer=consumer,
        output=output,
    )

    return sflow
