import os
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

DEFAULT_INFO = {
    "ONTUNEDB_HOST": "localhost",
    "ONTUNEDB_PORT": 5432,
    "ONTUNEDB_DBNAME": "ontune",
    "ONTUNEDB_USERNAME": "ontune",
    "ONTUNEDB_PASSWORD": "ontune",
    "ONTUNEDB_MANAGERNAME": "onTuneKubeManager"
}

#ONTUNEDB_HOST = DEFAULT_INFO["ONTUNEDB_HOST"]
ONTUNEDB_HOST = os.environ["ONTUNEDB_HOST"] if "ONTUNEDB_HOST" in os.environ else DEFAULT_INFO["ONTUNEDB_HOST"]
ONTUNEDB_PORT = int(os.environ["ONTUNEDB_PORT"]) if "ONTUNEDB_PORT" in os.environ else DEFAULT_INFO["ONTUNEDB_PORT"]
ONTUNEDB_DBNAME = os.environ["ONTUNEDB_DBNAME"] if "ONTUNEDB_DBNAME" in os.environ else DEFAULT_INFO["ONTUNEDB_DBNAME"]
ONTUNEDB_USERNAME = os.environ["ONTUNEDB_USERNAME"] if "ONTUNEDB_USERNAME" in os.environ else DEFAULT_INFO["ONTUNEDB_USERNAME"]
ONTUNEDB_PASSWORD = os.environ["ONTUNEDB_PASSWORD"] if "ONTUNEDB_PASSWORD" in os.environ else DEFAULT_INFO["ONTUNEDB_PASSWORD"]
ONTUNEDB_MANAGERNAME = os.environ["ONTUNEDB_MANAGERNAME"] if "ONTUNEDB_MANAGERNAME" in os.environ else DEFAULT_INFO["ONTUNEDB_MANAGERNAME"]

class DB:
    def __init__(self, auto_connection=False):
        self._host = ONTUNEDB_HOST
        self._port = ONTUNEDB_PORT
        self._dbname = ONTUNEDB_DBNAME
        self._username = ONTUNEDB_USERNAME
        self._password = ONTUNEDB_PASSWORD
        self._mgrname = ONTUNEDB_MANAGERNAME

        if auto_connection:
            self.create_connection()

    def create_connection(self):
        self._connection_pool = pool.ThreadedConnectionPool(1, 32, host=self._host, port=self._port, database=self._dbname, user=self._username, password=self._password)

    def shutdown_connection_pool(self):
        if self._connection_pool is not None:
            self._connection_pool.closeall()

    def get_basic_info(self, info_type):
        if info_type == "host":
            return self._host
        elif info_type == "managername":
            return self._mgrname
        else:
            return None
            
    @contextmanager
    def get_resource_rdb(self, autocommit=False):
        conn = self._connection_pool.getconn()
        conn.autocommit = autocommit
        cursor = conn.cursor()
        cur_dict = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            yield cursor, cur_dict, conn
        finally:
            cursor.close()
            self._connection_pool.putconn(conn)