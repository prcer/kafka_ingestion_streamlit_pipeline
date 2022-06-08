from sqlalchemy import create_engine, text
import logging as log

def connect(pg_url):
    try:
        return create_engine(pg_url)
    except Exception as e:
        log.error(f'Unable to create Postgres engine: {str(e)}')
        return None


def insert_tuple(pg_engine, schema, table, val_tuple, idx='0'):
    values = ",".join([str(val) if not isinstance(val, str) else f"'{val}'" for val in val_tuple])
    query = f'INSERT INTO {schema}.{table} VALUES ({values})'
    try:
        with pg_engine.connect() as conn:
            conn.execute(text(query))
        return True
    except Exception as e:
        log.error(f'{idx} - Unable to insert into Postgres table {schema}.{table}: {str(e)}')
        return False


def setup_table(pg_engine):
    try:
        with pg_engine.connect() as conn:
            with open('/opt/science/setup_table.sql') as sql_file:
                query = text(sql_file.read())
                conn.execute(query)
    except Exception as e:
        log.error(f'Unable to setup Postgres schema and table: {str(e)}')