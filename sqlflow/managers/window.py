import logging
import json
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
    def __init__(self, conn, table: Table, size_seconds, sink: Sink):
        self.conn = conn
        self.table = table
        self.size_seconds = size_seconds
        self.sink = sink
        self._poll_interval_seconds = 10
        self.serde = JSON()
        self._stopped = None

    def stop(self):
        self._stopped = True

    def collect_closed(self) -> [object]:
        # select all data with 'closed' windows.
        # 'closed' is identified by times earlier than NOW() - size_seconds
        import ipdb; ipdb.set_trace();
        stmt = '''
        SELECT 
            * 
        FROM {}
        WHERE 
            {} < CURRENT_TIMESTAMP - INTERVAL '{}' SECOND
        '''.format(
            self.table.name,
            self.table.time_field,
            self.size_seconds,
        )
        logger.debug(stmt)
        self.conn.begin()
        df = self.conn.execute(stmt).df()

        records = json.loads(
            df.to_json(
                orient='records',
                index=False,
            )
        )
        self.conn.commit()
        return records

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
            {} < CURRENT_TIMESTAMP - INTERVAL '{}' SECOND
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
        logger.info('checking for closed windows')
        closed_records = self.collect_closed()
        logger.info('found: {} closed records'.format(len(closed_records)))
        if closed_records:
            self.flush(closed_records)
        # get the max record present and delete from there
        self.delete_closed()

    def start(self):
        logger.info('starting managers thread')
        while not self._stopped:
            self.poll()
            time.sleep(self._poll_interval_seconds)
