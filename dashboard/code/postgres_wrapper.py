from sqlalchemy import create_engine
import logging as log
import pandas as pd

def connect(pg_url):
    try:
        return create_engine(pg_url)
    except Exception as e:
        log.error(f'Unable to create Postgres engine: {str(e)}')
        return None


def read_data(pg_engine, schema, table):
    # Select only the columns actually used
    query = f"SELECT prvdr_spec, srvc_desc, tot_users, avg_chrg_amt, avg_prcnt_mdcr_amt, prvdr_state \
              FROM {schema}.{table}"
    try:
        with pg_engine.connect() as conn:
            df = pd.read_sql_query(query, con=pg_engine)
        return df
    except Exception as e:
        log.error(f'Unable to read data from Postgres table {schema}.{table}: {str(e)}')
        return None