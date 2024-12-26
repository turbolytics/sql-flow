import os.path

SQL_RESULTS_CACHE_DIR = os.environ.get(
    'SQLFLOW_SQL_RESULTS_CACHE_DIR',
    os.path.join(
        '/tmp',
        'sqlflow',
        'resultscache',
    ),
)

STATIC_ROOT = os.environ.get(
    'SQLFLOW_STATIC_ROOT',
    os.path.join(
        '/tmp',
        'sqlflow',
        'static',
    ),
)

LOG_LEVEL = os.environ.get('SQLFLOW_LOG_LEVEL', 'INFO')

VARS = {
    'STATIC_ROOT': STATIC_ROOT,
    'SQL_RESULTS_CACHE_DIR': SQL_RESULTS_CACHE_DIR
}

DEV_DIR = os.path.join(
    os.path.dirname(__file__),
    '..',
    'dev',
)

CONF_DIR = os.path.join(DEV_DIR, 'config')
FIXTURES_DIR = os.path.join(DEV_DIR, 'fixtures')
