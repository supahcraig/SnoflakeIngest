import os, sys, logging
import uuid
import json
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle = 'qmark'


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"

    p_key = serialization.load_pem_private_key(bytes(private_key, 'utf-8'),
                                               password=None)

    pkb = p_key.private_bytes(encoding=serialization.Encoding.DER,
                              format=serialization.PrivateFormat.PKCS8,
                              encryption_algorithm=serialization.NoEncryption())

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv('SNOWFLAKE_USER'),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-copy-into'},
    )

    snow.cursor().execute('use schema INGEST')

    return conn

def save_to_snowflake(snow, batch, temp_dir):
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

    arrow_table = pa.Table.from_pandas(pandas_df)
    out_path = f'{temp_dir.name}/{str(uuid.uuid1())}.parquet'

    pq.write_table(arrow_table, out_path, use_dictionary=False, compression="snappy")
    snow.cursor().execute("put 'file:///{0}' @%lift_tickets_py_copy_into".format(out_path))

    os.unlink(out_path)

    snow.cursor().execute("copy into lift_tickets_py_copy_into file_format=(TYPE='PARQUET') match_by_column_name=case_insensitive purge=true")

    logging.debug(f'inserted {len(batch)} tickets')


if __name__ == '__main__':
    args = sys.argv[1:]
    batch_size = int(args[0])

    snow = connect_snow()

    batch = []
    temp_dir = tempfile.TemporaryDirectory()

    for message in sys.stdin:
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
                save_to_snowflake(snow, batch, temp_dir)
                batch = []

        else:
            break

    # this handles the final batch
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir)

    temp_dir.cleanup()
    snow.close()
    logging.info('ingest complete')

