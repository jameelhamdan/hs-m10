import os
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
import snowflake.connector
import pandas as pd
from config import Config


SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def migrate_table_data(source_engine, sf_conn, table_name):
    snowflake_table_name = f'EVENTHORIZON_{table_name.upper()}'
    with source_engine.connect() as conn:
        df = pd.read_sql_table(table_name, conn)

    df.columns = map(lambda x: str(x).upper(), df.columns)

    for col in df.select_dtypes(include=['datetime', 'datetime64']).columns:
        df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

    if df.empty:
        print(f"No data found in source for {snowflake_table_name}")
        return

    with sf_conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{snowflake_table_name}")
        pass

    success, nchunks, nrows, _ = write_pandas(
        conn=sf_conn,
        df=df,
        table_name=snowflake_table_name,
        schema=SNOWFLAKE_SCHEMA,
        database=SNOWFLAKE_DATABASE,
    )

    if success:
        print(f"Migrated {nrows} rows to {snowflake_table_name}")
    else:
        print(f"Failed to migrate {snowflake_table_name}")


def create_snowflake_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EVENTHORIZON_TOPIC (
                id INTEGER PRIMARY KEY,
                created_at TIMESTAMP_NTZ NOT NULL,
                name VARCHAR
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EVENTHORIZON_METRIC (
                id INTEGER PRIMARY KEY,
                created_at TIMESTAMP_NTZ NOT NULL,
                name VARCHAR,
                topic_ids ARRAY
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EVENTHORIZON_METRICVALUE (
                id INTEGER PRIMARY KEY,
                created_at TIMESTAMP_NTZ NOT NULL,
                value NUMERIC(10,4),
                calculated_on TIMESTAMP_NTZ,
                metric_id INTEGER,
                FOREIGN KEY (metric_id) REFERENCES EVENTHORIZON_METRIC(id)
            )
        """)


if __name__ == "__main__":
    source_engine = create_engine(
        Config.DATABASE_URL,
        connect_args={
            'options': '-csearch_path={}'.format(Config.DB_SCHEMA)
        },
    )

    sf_conn = get_snowflake_connection()

    try:
        create_snowflake_tables(sf_conn)

        migrate_table_data(source_engine, sf_conn, 'topic')
        migrate_table_data(source_engine, sf_conn, 'metric')
        migrate_table_data(source_engine, sf_conn, 'metricvalue')

        print("Migration completed successfully!")

    finally:
        sf_conn.close()
        source_engine.dispose()
