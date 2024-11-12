import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile

load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"

    p_key = serialization.load_pem_private_key(bytes(private_key, 'utf-8'),
                                               password=None)

    pkb = p_key.private_bytes(encoding=serialization.Encoding.DER,
                              format=serialization.PrivateFormat.PKCS8,
                              encryption_algorithm=serialization.NoEncryption())

    logging.info('Establishing connection to Snowflake...')
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv('SNOWFLAKE_USER'),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-copy-into'},
    )

    conn.cursor().execute('use schema INGEST')

    return conn


def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    logging.debug('inserting batch to db')

    pandas_df = pd.DataFrame(
        batch,
        columns=[
            'txid',
            'rfid',
            'resort',
            'purchase_time',
            'expiration_time',
            'days',
            'name',
            'address',
            'phone',
            'email',
            'emergency_contact'
        ]
    )

    #file_name = f"{str(uuid.uuid1())}.parquet"
    #out_path = f"{temp_dir.name}/{file_name}"
    #pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')


    file_name = f'{str(uuid.uuid1())}.parquet'
    out_path = f'{temp_dir.name}/{file_name}'

    arrow_table = pa.Table.from_pandas(df=pandas_df)
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')

    snow.cursor().execute(f"PUT 'file:///{out_path}' @%lift_tickets_py_snowpipe")
    os.unlink(out_path)

    resp = ingest_manager.ingest_files([StagedFile(file_name, None),])
    logging.info(f'response from snowflake for file {file_name}: {resp["responseCode"]}')


if __name__ == '__main__':
    logging.debug('starting snowpipe...')
    args = sys.argv[1:]
    batch_size = int(args[0])

    snow = connect_snow()
    batch = []

    temp_dir = tempfile.TemporaryDirectory()

    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
    ingest_manager = SimpleIngestManager(account=os.getenv("SNOWFLAKE_ACCOUNT"),
                                         host=host,
                                         user=os.getenv("SNOWFLAKE_USER"),
                                         pipe='INGEST.INGEST.LIFT_TICKETS_PIPE',
                                         private_key=private_key)

    for message in sys.stdin:
        logging.debug('processing a message')
        if message != '\n':
            record = json.loads(message)

            batch.append(
                (
                    record["txid"],
                    record["rfid"],
                    record["resort"],
                    record["purchase_time"],
                    record["expiration_time"],
                    record["days"],
                    record["name"],
                    record["address"],
                    record["phone"],
                    record["email"],
                    record["emergency_contact"],
                )
            )

            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir, ingest_manager)
                batch = []
            else:
                break

        if len(batch) > 0:
            save_to_snowflake(snow, batch, temp_dir, ingest_manager)

        temp_dir.cleanup()
        snow.close()
        logging.info('ingest complete')
else:
    logging.info('not in main')
