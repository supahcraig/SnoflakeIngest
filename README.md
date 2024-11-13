# How to Ingest Data into Snowflake


## General Notes

* There are several places where `CASE_INSENSITIVE` or `CASE_SENSITIVE` appears in either DDL or python.   If you find that your rows are being inserted but all the values are null, double check that the case of your column lists corresponds to your case (in)sensitive setting.   It would appear that behind the scenes it is querying the snowflake data dictionary to discover the column names to do the mapping, and those are stored in all upper case.   But the python in this repo has the column names in lower case when it builds the data frame, so if I have the Snowflake Task or COPY command set to `CASE_SENSITIVE` it will insert a row, but all the fields will be null since it is looking for an _exact match_ on column name, including case.

* 


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

### Tips

* Ingest is billed based on warehouse credits consumed while online.
* The connectors support multi-inserts but data containing a variant field cannot be formatted into a multi-insert.
* Using inserts and multi-inserts will not efficiently use warehouse resources (optimal at 100MB or more with some concurrency). It is better to upload data and COPY into the table.
* Connectors will switch to creating and uploading a file and doing a COPY into when large batches are set. This is not configurable.
* Many assume adding significant concurrency will support higher throughputs of data. The additional concurrent INSERTS will be blocked by other INSERTS, more frequently when small payloads are inserted. You need to move to bigger batches to get more througput.
* Review query history to see what the connector is doing.
* In cases where the connector has enough data in the executemany to create a well sized file for COPY and does so, this does become as efficient as the following methods.

The example above could not use executemany as it had VARIANT data.

The next methods will show how to batch into better sized blocks of work which will drive higher throughputs and higher efficiency on Snowflake.


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

### Tips

This last call will batch together 10,000 records into each file for processing. As this file gets larger, up to 100mb, you will see this be more efficient on seconds of compute used in Snowpipe and see higher throughputs. Feel free to generate more test data and increase this to get more understanding of this relationship. Review the query performance in Query History in Snowflake.

* Ingest is billed based on warehouse credits consumed while online.
* It is very hard to fully utilize a warehouse with this pattern. Adding some concurrency will help IF the files are already well sized. Even with the best code, very few workloads have fixed data flow volumes that well match a warehouse. This is mostly a wasted effort as serverless and snowpipe solves all use cases w/o constraints.
* Try to get to 100mb files for most efficiency.
* Best warehouses sizes are almost always way smaller than expected, commonly XS.

---


## File upload/copy using Snowpipe

This is similar to the regular file upload/copy, except that it uploads the file to a Snowflake stage, and then the stage will load that data into the table behind the scenes, asynchronously.  The files are loaded into an implicit stage (i.e. `@%table_name`) where they will sit until the Ingest Manager runs the `ingest_files` method.  This specific python will call `ingest_files` each time a file is uploaded to the stage, but it's also possible to process all the files in the stage after the fact. 

_NOTE:_ It can take a minute or two for the data to be visible in your table.  


### Database setup

```sql
USE ROLE INGEST;
CREATE OR REPLACE TABLE LIFT_TICKETS_PY_SNOWPIPE (TXID varchar(255), RFID varchar(255), RESORT varchar(255), PURCHASE_TIME datetime, EXPIRATION_TIME date, DAYS number, NAME varchar(255), ADDRESS variant, PHONE varchar(255), EMAIL varchar(255), EMERGENCY_CONTACT variant);

CREATE PIPE LIFT_TICKETS_PIPE AS COPY INTO LIFT_TICKETS_PY_SNOWPIPE
FILE_FORMAT=(TYPE='PARQUET') 
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
```

### Python usage

`python ./data_generator.py 1 | python py_snowpipe.py 1`

```bash
cat data.json.gz | zcat | python py_snowpipe.py 10000
```

### Tips

This last call will batch together 10,000 records into each file for processing. As this file gets larger, up to 100mb, you will see this be more efficient on seconds of compute used in Snowpipe and see higher throughputs.

Test this approach with more test data and larger batch sizes. Review `INFORMATION_SCHEMA PIPE_USAGE_HISTORY` to see how efficient large batches are vs small batches.

Tips
* Ingest is billed based on seconds of compute used by Snowpipe and number of files ingested.
* This is one of the most efficient and highest throughput ways to ingest data when batches are well sized.
* File size is a huge factor for cost efficiency and throughput. If you have files and batches much smaller than 100mb and cannot change them, this pattern should be avoided.
* Expect delays when Snowpipe has enqueued the request to ingest the data. This process is asynchronous. In most cases these patterns can deliver ~ minute ingest times when including the time to batch, upload, and copy but this varies based on your use case.

---

## File upload/copy (Serverless)


### Database setup

```sql
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE INGEST;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE INGEST;

USE ROLE INGEST;
CREATE OR REPLACE TABLE LIFT_TICKETS_PY_SERVERLESS (TXID varchar(255), RFID varchar(255), RESORT varchar(255), PURCHASE_TIME datetime, EXPIRATION_TIME date, DAYS number, NAME varchar(255), ADDRESS variant, PHONE varchar(255), EMAIL varchar(255), EMERGENCY_CONTACT variant);

CREATE OR REPLACE TASK LIFT_TICKETS_PY_SERVERLESS 
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL' 
AS
COPY INTO LIFT_TICKETS_PY_SERVERLESS
FILE_FORMAT=(TYPE='PARQUET') 
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE 
PURGE=TRUE;
```

### Python usage

`python ./data_generator.py 1 | python py_serverless.py 1`

```bash
cat data.json.gz | zcat | python py_serverless.py 10000
```

### Tips

If you run multiple tests with different batch sizes (especially smaller sizes), you will see this can save credit consumption over the previous Snowpipe solution as it combines files into loads.

The code is calling execute task after each file is uploaded. While this may not seem optimimal, it is not running after each file is uploaded. It is leveraging a feature of tasks which does not allow additional tasks to be enqueued when one is already enqueued to run.

It is also common to schedule the task to run every n minutes instead of calling from the clients.


* Only run the Task as needed when enough data (> 100mb) has been loaded into stage for most efficiency.
* Use Serverless Tasks to avoid per file charges and resolve small file inefficiencies.

---

## Via Snowpark Dataframe

### Database setup

```sql
USE ROLE INGEST;
CREATE OR REPLACE TABLE LIFT_TICKETS_PY_SNOWPARK (TXID varchar(255), RFID varchar(255), RESORT varchar(255), PURCHASE_TIME datetime, EXPIRATION_TIME date, DAYS number, NAME varchar(255), ADDRESS variant, PHONE varchar(255), EMAIL varchar(255), EMERGENCY_CONTACT variant);
```

### Python usage

`python data_generator.py 1 | python py_snowpark.py 1`

```bash
cat data.json.gz | zcat | python py_snowpark.py 10000
```

### Tips

* Ingest is billed based on warehouse credits consumed while online.
* Most efficient when batches get closer to 100mb.
* Great for when data has been processed using DataFrames.



