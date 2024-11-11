import os, sys, logging
import json
import snowflake.connector

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

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv('SNOWFLAKE_USER'),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-insert'},
    )


def save_to_snowflake(snow, message):
    logging.debug('inserting record to db')

    record = json.loads(message)

    row = (record['txid'], record['rfid'], record["resort"], record["purchase_time"], record["expiration_time"],
           record['days'], record['name'], json.dumps(record['address']), record['phone'], record['email'],
           json.dumps(record['emergency_contact']))

    snow.cursor().execute('insert into ingest.LIFT_TICKETS_PY_INSERT (txid, rfid, resort, purchase_time, expiration_time, days, name, address, phone, email, emergency_contact) SELECT ?,?,?,?,?,?,?,PARSE_JSON(?),?,?,PARSE_JSON(?)', row)

    logging.debug(f'inserted ticket {record}')

if __name__ == '__main__':
    snow = connect_snow()

    for message in sys.stdin:
        if message != '\n':
            save_to_snowflake(snow, message)
        else:
            break

    snow.close()
    logging.info('ingest complete.')
