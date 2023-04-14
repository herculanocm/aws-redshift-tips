from typing import Dict
from sys import stdout
import logging
from de_redshift_tips.custom_formatter_logger import CustomFormatter
from redshift_connector import connect, Connection
from boto3 import client
import pg8000 as pg

def get_logger(logger: logging.Logger = None) -> logging.Logger:
    if logger is None:

        handler = logging.StreamHandler(stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(CustomFormatter())

        logger = logging.getLogger(__name__)

        if logger.hasHandlers():
            logger.removeHandler(handler)

        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    return logger


def redshift_open_connection(host: str, port: int, database: str, user: str, password: str) -> Connection:
    conn = connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    return conn

def redshift_pg8000_open_connection(host: str, port: int, database: str, user: str, password: str) -> Connection:
    conn = pg.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    return conn


def get_connection_glue(glue_client: client, glue_connection_name: str) -> Dict:
    logger = logging.getLogger()
    logger.debug(f'Getting connection glue: {glue_connection_name}')

    response = glue_client.get_connection(Name=glue_connection_name)
    jdbc_url = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    list_jdbc = jdbc_url.split('/')
    database_port = list_jdbc[2].split(':')

    conn_param = {}
    conn_param['jdbc_url'] = jdbc_url
    conn_param['url'] = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    conn_param['database'] = list_jdbc[3]
    conn_param['hostname'] = database_port[0]
    conn_param['port'] = int(database_port[1])
    conn_param['user'] = response['Connection']['ConnectionProperties']['USERNAME']
    conn_param['password'] = response['Connection']['ConnectionProperties']['PASSWORD']

    return conn_param


def redshift_open_connection_by_glue(glue_client: client, glue_connection_name: str, database: str = None) -> Connection:
    logger = logging.getLogger()
    conn_param = get_connection_glue(glue_client, glue_connection_name)
    logger.debug(f'Openning connection by glue params')

    if database is None:
        database = conn_param['database']

    return redshift_open_connection(
        conn_param['hostname'],
        conn_param['port'],
        database,
        conn_param['user'],
        conn_param['password']
    )

def redshift_pg8000_open_connection_by_glue(glue_client: client, glue_connection_name: str, database: str = None) -> Connection:
    logger = logging.getLogger()
    conn_param = get_connection_glue(glue_client, glue_connection_name)
    logger.debug(f'Openning connection by glue params')

    if database is None:
        database = conn_param['database']

    return redshift_pg8000_open_connection(
        conn_param['hostname'],
        conn_param['port'],
        database,
        conn_param['user'],
        conn_param['password']
    )

def redshift_get_rows_result_query(glue_client: client, glue_connection_name: str, str_query: str, database: str = None) -> list:
    logger = logging.getLogger()
    conn = redshift_open_connection_by_glue(glue_client, glue_connection_name, database)
    conn.autocommit = False
    cur = conn.cursor()
    
    logger.debug(f'Executing query')
    result = []
    cur.execute(str_query)
    cols = [a[0] for a in cur.description]
    for row in cur.fetchall():
        result.append({a: b for a,b in zip(cols, row)})
                
    cur.close()
    cur.close()
    
    return result

def redshift_exec_query_with_commit(glue_client: client, glue_connection_name: str, str_query: str, database: str = None):
    logger = logging.getLogger()
    conn = redshift_open_connection_by_glue(glue_client, glue_connection_name, database)
    cur = conn.cursor()
    logger.debug(f'Executing query with commit')
    cur.execute(str_query)
    conn.commit()
    cur.close()
    cur.close()

def redshift_pg8000_exec_query_with_commit(glue_client: client, glue_connection_name: str, str_query: str, database: str = None):
    logger = logging.getLogger()
    conn = redshift_pg8000_open_connection_by_glue(glue_client, glue_connection_name, database)
    cur = conn.cursor()
    logger.debug(f'Executing query with commit')
    cur.execute(str_query)
    conn.commit()
    cur.close()
    cur.close()

def redshift_pg8000_get_rows_result_query(glue_client: client, glue_connection_name: str, str_query: str, database: str = None) -> list:
    logger = logging.getLogger()
    conn = redshift_pg8000_open_connection_by_glue(glue_client, glue_connection_name, database)
    cur = conn.cursor()
    
    logger.debug(f'Executing query')
    result = []
    cur.execute(str_query)
    cols = [a[0] for a in cur.description]
    for row in cur.fetchall():
        result.append({a: b for a,b in zip(cols, row)})
                
    cur.close()
    cur.close()
    
    return result
