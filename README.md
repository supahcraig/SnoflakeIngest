# How to Ingest Data into Snowflake


## Data Generator

This will generate `n` rows of randomized json using the Faker package.

`python data_generator <n>`

Generating 100k rows:

```bash
python data_generator.py 100000 >| gzip > data.json.gz
```

---

## Database Setup

```sql
CREATE WAREHOUSE INGEST;
CREATE ROLE INGEST;
GRANT USAGE ON WAREHOUSE INGEST TO ROLE INGEST;
GRANT OPERATE ON WAREHOUSE INGEST TO ROLE INGEST;
CREATE DATABASE INGEST;
CREATE SCHEMA INGEST;
GRANT OWNERSHIP ON DATABASE INGEST TO ROLE INGEST;
GRANT OWNERSHIP ON SCHEMA INGEST.INGEST TO ROLE INGEST;

CREATE USER INGEST PASSWORD='<REDACTED>' LOGIN_NAME='INGEST' MUST_CHANGE_PASSWORD=FALSE, DISABLED=FALSE, DEFAULT_WAREHOUSE='INGEST', DEFAULT_NAMESPACE='INGEST.INGEST', DEFAULT_ROLE='INGEST';
GRANT ROLE INGEST TO USER INGEST;
GRANT ROLE INGEST TO USER <YOUR_USERNAME>;
```

### Public/Private Key generation

```bash
openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
PUBK=`cat ./rsa_key.pub | grep -v KEY- | tr -d '\012'`
echo "ALTER USER INGEST SET RSA_PUBLIC_KEY='$PUBK';"
```

That will output an `ALTER USER` statement that will attach your public key to your Snowflake user.

### Build out .env file

Put your private key in your env file (it's possible to use a pem file here but I don't know the exact syntax).

```bash
PRVK=`cat ./rsa_key.p8 | grep -v KEY- | tr -d '\012'`
echo "PRIVATE_KEY=$PRVK"
```

And then flush out the rest of your `.env` file with this info:

```yaml
SNOWFLAKE_ACCOUNT=<ACCOUNT_HERE>
SNOWFLAKE_USER=INGEST
PRIVATE_KEY=<PRIVATE_KEY_HERE>
```

---

## Inserting directly to a Snowflake table

This just builds out insert statements and executes them one at time.   If the insert payload has variant columns, "executeMany" is not possible.

### Database setup

USE ROLE INGEST;
CREATE OR REPLACE TABLE LIFT_TICKETS_PY_INSERT (TXID varchar(255), RFID varchar(255), RESORT varchar(255), PURCHASE_TIME datetime, EXPIRATION_TIME date, DAYS number, NAME varchar(255), ADDRESS variant, PHONE varchar(255), EMAIL varchar(255), EMERGENCY_CONTACT variant);


### Python usage

`python ./data_generator.py 1 | python py_insert.py`

```bash
cat data.json.gz | zcat | head -n 100 | python py_insert.py
```

---

## File upload/copy

This method involves writing batches by going through these steps:

1.  Turning an array of "rows" into a pandas dataframe
2.  Turning the dataframe into a an Arrow table
3.  writing out that Arrow table to a compressed file in a temp directory
4.  Executing a `PUT` to get the file into an "implicit" (?) Snowflake stage
5.  Executing a `COPY INTO` from the implicit stage into the actual table.

### Database setup

```sql
USE ROLE INGEST;
CREATE OR REPLACE TABLE LIFT_TICKETS_PY_COPY_INTO (TXID varchar(255), RFID varchar(255), RESORT varchar(255), PURCHASE_TIME datetime, EXPIRATION_TIME date, DAYS number, NAME varchar(255), ADDRESS variant, PHONE varchar(255), EMAIL varchar(255), EMERGENCY_CONTACT variant);
```


### Python usage

`python ./data_generator.py 1 | python py_copy_into.py 1`

```bash
cat data.json.gz | zcat | python py_copy_into.py 10000
```


---


## File upload/copy using Snowpipe



### Database setup

```sql
USE ROLE INGEST;
CREATE OR REPLACE TABLE LIFT_TICKETS_PY_SNOWPIPE (TXID varchar(255), RFID varchar(255), RESORT varchar(255), PURCHASE_TIME datetime, EXPIRATION_TIME date, DAYS number, NAME varchar(255), ADDRESS variant, PHONE varchar(255), EMAIL varchar(255), EMERGENCY_CONTACT variant);

CREATE PIPE LIFT_TICKETS_PIPE AS COPY INTO LIFT_TICKETS_PY_SNOWPIPE
FILE_FORMAT=(TYPE='PARQUET') 
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
```



