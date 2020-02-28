
import boto3
import logging
import os
import psycopg2
from tempfile import TemporaryFile

from extract.utils import snowflake_engine_factory, execute_query

class SnowflakeToPgDataTypeMapper(object):
    DATA_TYPE_MAP = {
        "number": "integer",
        "timestamp_ntz": "timestamp without time zone",
        "text": "varchar"
    }

    @classmethod
    def get_pg_type(cls, sf_type):
       return cls.DATA_TYPE_MAP.get(sf_type, sf_type)


class PgImporter(object):
    def __init__(self, source_table, destination_table, post_process_file):
        self.source_schema, self.source_table = source_table.split(".")
        self.destination_schema, self.destination_table = destination_table.split(".")
        self.sf_engine = snowflake_engine_factory(os.environ, "TRANSFORMER", self.source_schema)
        self.post_process_file = post_process_file

    def run(self):
        file_name = self._unload_to_s3()
        return self._process_on_pg(file_name)

    def _create_pg_table(self, conn):
        column_specs = self._get_table_columns()

        columns = []
        for column in column_specs:
            columns.append(f"{column[0].lower()} {SnowflakeToPgDataTypeMapper.get_pg_type(column[1].lower())}")

        column_query = ", ".join(columns)

        create_table = f"create table if not exists {self.destination_schema}.{self.destination_table} ({column_query});"

        conn.execute(create_table)

    def _get_table_columns(self):
        with self.sf_engine.begin() as conn:
            column_query = f"""
                select
                    column_name,
                    data_type
                from analytics.information_schema.columns
                where table_schema = '{self.source_schema.upper()}' and table_name = '{self.source_table.upper()}'
                order by ordinal_position;
            """
            return conn.execute(column_query).fetchall()

    def _unload_to_s3(self):
        sf_unload_stage = os.getenv('SNOWFLAKE_UNLOAD_STAGE', 'analytics.mattermost.pg_stage')
        file_name = f"{self.source_table}.csv"

        with self.sf_engine.begin() as conn:
            conn.execute(f"COPY INTO @{sf_unload_stage}/{file_name} from {self.source_schema}.{self.source_table} overwrite=true single=true;")

        return file_name

    def _process_on_pg(self, file_name):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        bucket = os.getenv('PG_IMPORT_BUCKET')
        row_count = 0

        with TemporaryFile() as f:
            s3_client.download_fileobj(bucket, f"pg_unload_stage/{file_name}", f)
            f.seek(0)
            conn = None
            try:
                conn = psycopg2.connect(os.getenv('HEROKU_POSTGRESQL_URL'))
                cursor = conn.cursor()
                self._create_pg_table(cursor)
                full_table = f"{self.destination_schema}.{self.destination_table}"
                cursor.execute(f"delete from {full_table};")
                cursor.copy_expert(f"COPY {full_table} FROM STDIN WITH CSV", f)

                with open(f"transform/sql/{self.post_process_file}.sql") as sql_file:
                    queries = sql_file.read().split(";")
                    for query in queries:
                        if query.strip() == '':
                            continue
                        cursor.execute(query)
                        row_count += cursor.rowcount

                conn.commit()
                cursor.close()
            finally:
                if conn is not None:
                    conn.close()

        s3_client.delete_object(Bucket=bucket, Key=file_name)

        return row_count

class PgImportProcessor(object):
    def __init__(self, processing_table='analytics.util.pg_imports'):
        self.processing_table = processing_table
        self.sf_engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")

    def run(self):
        for record in self._get_tables_to_process():
            id, source_table, destination_table, post_process_file = record
            logging.info(f"Running PG import for id {id} with source table {source_table}")
            rows_affected = PgImporter(source_table, destination_table, post_process_file).run()
            self._update_processed_at(id, rows_affected)

    def _get_tables_to_process(self):
        with self.sf_engine.begin() as conn:
            return conn.execute(f"""
                SELECT
                    id,
                    source_table,
                    destination_table,
                    post_process_file
                FROM {self.processing_table}
                WHERE processed_at IS NULL;
            """).fetchall()


    def _update_processed_at(self, id, rows_affected):
        with self.sf_engine.begin() as conn:
            conn.execute(f"""
                UPDATE analytics.util.pg_imports
                SET processed_at = current_timestamp,
                    rows_affected = {rows_affected}
                WHERE id = {id};
            """)
