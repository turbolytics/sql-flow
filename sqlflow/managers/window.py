import logging
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlflow.sinks import Sink
from sqlflow.serde import JSON

logger = logging.getLogger(__name__)


@dataclass
class Table:
    name: str
    time_field: str


class Tumbling:
    """
    Tumbling managers handler manages the table provided. Management includes:
    - Polling table for records outside the managers range.
    - Publishing records that have closed.
    - Deleteting closed records from the table.
    """
    def __init__(self,
                 conn,
                 collect_closed_windows_sql,
                 delete_closed_windows_sql,
                 poll_interval_seconds,
                 sink: Sink,
                 lock=threading.Lock()):
        self.conn = conn
        self.collect_closed_windows_sql = collect_closed_windows_sql
        self.delete_closed_windows_sql = delete_closed_windows_sql
        self.poll_interval_seconds = poll_interval_seconds
        self.sink = sink
        self.serde = JSON()
        self._stopped = None
        self._lock = lock

    def stop(self):
        self._stopped = True

    def collect_closed(self) -> [object]:
        # select all data with 'closed' windows.
        df = self.conn.execute(self.collect_closed_windows_sql).fetch_arrow_table()
        return df.to_pylist()

    def flush(self, records):
        """
        Flush writes all closed records.

        :return:
        """
        for record in records:
            self.sink.write(
                val=self.serde.encode(record)
            )

    def delete_closed(self) -> int:
        """
        Delete all closed windows.

        :return: the number of deleted rows
        """
        res = self.conn.execute(self.delete_closed_windows_sql).fetchall()
        return res[0][0]

    def poll(self):
        """
        Poll will check the current table state for closed windows.

        :return:
        """
        t = datetime.now(tz=timezone.utc)
        # take the lock
        with self._lock:
            closed_records = self.collect_closed()

        logger.debug('found: {} closed records'.format(len(closed_records)))
        if closed_records:
            self.flush(closed_records)
            # get the max record present and delete from there
            with self._lock:
                self.delete_closed()

    def start(self):
        logger.info('starting managers thread')
        while not self._stopped:
            self.poll()
            time.sleep(self.poll_interval_seconds)
