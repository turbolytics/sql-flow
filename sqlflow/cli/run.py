import os
import sys
from datetime import datetime, timezone

from sqlflow import new_from_path, InferredBatch
from confluent_kafka import Consumer, KafkaError, KafkaException


def start(config):
    conf = new_from_path(config)
    # build the sqlflow instance

    # start the consume loop

    kconf = {
        'bootstrap.servers': ','.join(conf.kafka.brokers),
        'group.id': conf.kafka.group_id,
        'auto.offset.reset': conf.kafka.auto_offset_reset,
        'enable.auto.commit': False,
    }

    # start the kafka consumer
    consumer = Consumer(kconf)

    batch_file = os.path.join(
        conf.sql_results_cache_dir,
        'consumer_batch.json',
    )
    f = open(batch_file, 'w+')

    # iterate over batches
    try:
        consumer.subscribe(conf.pipeline.input.topics)

        num_messages = 0
        start_dt = datetime.now(timezone.utc)
        total_messages = 0
        while True:
            msg = consumer.poll(timeout=1.0)
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

            if num_messages == conf.pipeline.input.batch_size:
                f.flush()
                f.close()

                # apply the pipeline
                b = InferredBatch(conf)
                res = b.invoke(batch_file)
                # handle the output
                # for l in res:
                # print(l)
                # commit the offset
                # TODO - only commit after all messages in batch are processed

                consumer.commit(asynchronous=False)
                # reset the file state
                f = open(batch_file, 'w+')
                f.truncate()
                f.seek(0)
                num_messages = 0

    finally:
        consumer.close()