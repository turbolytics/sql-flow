import logging
import json
import threading
import time
from dataclasses import dataclass

from sqlflow.outputs import Writer

logger = logging.getLogger(__name__)


@dataclass
class Table:
    name: str
    time_field: str


class Tumbling:
    """
    Tumbling window handler manages the table provided. Management includes:
    - Polling table for records outside the window range.
    - Publishing records that have closed.
    - Deleteting closed records from the table.
    """
    def __init__(self, conn, table: Table, size_seconds, writer: Writer):
        self.conn = conn
        self.table = table
        self.size_seconds = size_seconds
        self.writer = writer
        self.poll_interval_seconds = 1
        self.lock = threading.Lock()

    def collect_closed(self) -> [object]:
        # select all data with 'closed' windows.
        # 'closed' is identified by times earlier than NOW() - size_seconds
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
        res = self.conn.execute(stmt)

        df = res.df()

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
            self.writer.write(record)

    def delete_closed(self, t):
        """
        Delete all closed windows.

        :return:
        """
        stmt = '''
        DELETE FROM {} WHERE {} < {}
        '''.format(
            self.table.name,
            self.table.time_field,
            t,
        )
        print(stmt)
        logger.debug(stmt)
        res = self.conn.execute(stmt).fetchall()
        print(res)

    def poll(self):
        """
        Poll will check the current table state for closed windows.

        :return:
        """
        # take the lock
        with self.lock:
            logger.debug('checking for closed windows')

    def start(self):
        while True:
            self.poll()
            time.sleep(self.poll_interval_seconds)
