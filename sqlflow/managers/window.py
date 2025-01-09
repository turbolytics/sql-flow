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
    def __init__(self, conn, table: Table, size_seconds, sink: Sink, lock=threading.Lock()):
        self.conn = conn
        self.table = table
        self.size_seconds = size_seconds
        self.sink = sink
        self._poll_interval_seconds = 10
        self.serde = JSON()
        self._stopped = None
        self._lock = lock

    def stop(self):
        self._stopped = True

    def collect_closed(self) -> [object]:
        # select all data with 'closed' windows.
        # 'closed' is identified by times earlier than NOW() - size_seconds
        stmt = '''
        SELECT 
            * 
        FROM {}
        WHERE 
            {} AT TIME ZONE 'utc' < (now()::timestamptz AT TIME ZONE 'UTC' - INTERVAL '{}' SECOND)
        '''.format(
            self.table.name,
            self.table.time_field,
            self.size_seconds,
        )
        logger.debug(stmt)
        df = self.conn.sql(stmt).to_arrow_table()
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
        stmt = '''
        DELETE 
            FROM {} 
        WHERE
            {} AT TIME ZONE 'utc' < (now()::timestamptz AT TIME ZONE 'UTC' - INTERVAL '{}' SECOND)
        '''.format(
            self.table.name,
            self.table.time_field,
            self.size_seconds,
        )
        logger.debug(stmt)
        res = self.conn.execute(stmt).fetchall()
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
            time.sleep(self._poll_interval_seconds)
