from flask import Flask, request, jsonify
from flask.views import View


class DebugAPI(View):
    init_every_request = False

    def __init__(self, conn, lock, *args, **kwargs):
        self._conn = conn
        self._lock = lock
        super(DebugAPI, self).__init__(*args, **kwargs)

    def dispatch_request(self):
        sql_query = request.args.get('sql')
        if not sql_query:
            return jsonify({'error': 'No SQL query provided'}), 400

        try:
            with self._lock:
                result = self._conn.execute(sql_query).fetchall()
            return jsonify(result)
        except Exception as e:
            return jsonify({'error': str(e)}), 500