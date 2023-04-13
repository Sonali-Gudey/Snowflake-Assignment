
--QUESTION-1
CREATE ROLE DEVELOPER;
CREATE ROLE ADMIN;
CREATE ROLE PII;

GRANT ROLE DEVELOPER TO ROLE ADMIN;
GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;

--QUESTION-2
CREATE OR REPLACE WAREHOUSE assignment_wh
WITH WAREHOUSE_SIZE = 'MEDIUM';

GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE ADMIN;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE ADMIN;

--QUESTION-3
USE ROLE ADMIN;

--QUESTION-4
CREATE DATABASE ASSIGNMENT_DB;
USE DATABASE ASSIGNMENT_DB;

--QUESTION-5
CREATE SCHEMA MY_SCHEMA;

--QUESTION-6
CREATE TABLE EMPLOYEE_DATA(
    id NUMBER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    age NUMBER,
    salary STRING,
    phone_number VARCHAR(255),
    etl_ts timestamp default current_timestamp(), -- for getting the time at which the record is getting inserted
    etl_by varchar default 'snowsight',-- for getting application name from which the record was inserted 
    file_name varchar -- for getting the name of the file used to insert data into the table.
);

--QUESTION-7
CREATE OR REPLACE FILE FORMAT my_json_format TYPE = JSON;

CREATE TABLE variant_table (                     
    variant_data variant
);

put file:///Users/sonali_gudey/Downloads/employee.json @%variant_table;

COPY INTO variant_table 
FROM @%variant_table file_format = MY_JSON_FORMAT;

select * from variant_table;


--QUESTION-8
CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT             
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

CREATE STAGE internal_stage file_format = my_csv_format;

--put file:///Users/sonali_gudey/Downloads/employee.csv @internal_stage;

DROP STORAGE INTEGRATION S3_INTEGRATION;

CREATE STORAGE INTEGRATION s3_integration
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::547795514214:role/sonali_role'
storage_allowed_locations = ('s3://snowflake-assignment001/csv_files/employee.csv');

GRANT ALL ON INTEGRATION s3_integration TO ROLE admin; 

DESC INTEGRATION s3_integration;

CREATE OR REPLACE STAGE external_stage
URL = 's3://snowflake-assignment001/csv_files/employee.csv'
--CREDENTIALS=(AWS_KEY_ID='AKIAX7CZIGNTPUEYB2K4' AWS_SECRET_KEY = 'o5eiPtLUNgK9Tr7bhrbqF06ZQUvuU5h+HkZWcqu/');
STORAGE_INTEGRATION = s3_integration
FILE_FORMAT = my_csv_format;

LIST @internal_stage;

LIST @external_stage;

DROP TABLE EMPLOYEE_EXTERNAL_STAGE;

--QUESTION-9
CREATE TABLE employee_internal_stage(
    id NUMBER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    age NUMBER,
    salary STRING,
    phone_number VARCHAR(255),
    etl_ts timestamp default current_timestamp(), -- for getting the time at which the record is getting inserted
    etl_by varchar default 'snowsight',-- for getting application name from which the record was inserted 
    file_name varchar -- for getting the name of the file used to insert data into the table.
);

CREATE TABLE employee_external_stage(
    id NUMBER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    age NUMBER,
    salary STRING,
    phone_number VARCHAR(255),
    etl_ts timestamp default current_timestamp(), -- for getting the time at which the record is getting inserted
    etl_by varchar default 'snowsight',-- for getting application name from which the record was inserted 
    file_name varchar -- for getting the name of the file used to insert data into the table.
);


COPY INTO employee_internal_stage(id, first_name, last_name, email, age, salary, phone_number,file_name)
FROM ( 
SELECT emp.$1, emp.$2, emp.$3, emp.$4, emp.$5, emp.$6, emp.$7, METADATA$FILENAME 
FROM @internal_stage/employee.csv.gz (file_format => my_csv_format) emp);

COPY INTO employee_external_stage(id, first_name, last_name, email, age, salary, phone_number,file_name)
FROM ( 
SELECT emp.$1, emp.$2, emp.$3, emp.$4, emp.$5, emp.$6, emp.$7, METADATA$FILENAME 
FROM @external_stage (file_format => my_csv_format) emp);

select * from employee_internal_stage limit 10;

select * from employee_external_stage limit 10;

--QUESTION-10
CREATE FILE FORMAT my_parquet_format TYPE = parquet;

CREATE STAGE parquet_stage file_format = my_parquet_format;

put file:///Users/sonali_gudey/Downloads/employee.parquet @parquet_stage;

SELECT * FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@parquet_stage',
      FILE_FORMAT=>'my_parquet_format'
    )
);

--QUESTION-11
SELECT * from @parquet_stage/employee.parquet; 

--QUESTION-12

--email_mask
CREATE OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string ->
CASE
WHEN CURRENT_ROLE() = 'PII' THEN VAL
ELSE '****MASK****'
END;

--contact_mask
CREATE OR REPLACE MASKING POLICY contact_mask AS (VAL string) RETURNS string -> 
CASE
WHEN CURRENT_ROLE() = 'PII' THEN VAL
ELSE '****MASK****'
END;

-- Adding the email mask policy to internal stage
ALTER TABLE IF EXISTS employee_internal_stage 
MODIFY EMAIL SET MASKING POLICY email_mask;

-- Adding the email mask policy to external stage
ALTER TABLE IF EXISTS employee_external_stage 
MODIFY EMAIL SET MASKING POLICY email_mask;

-- Adding the contact mask policy to internal stage
ALTER TABLE IF EXISTS employee_internal_stage 
MODIFY phone_number SET MASKING POLICY contact_mask;

-- Adding the contact mask policy to external stage
ALTER TABLE IF EXISTS employee_external_stage 
MODIFY phone_number SET MASKING POLICY contact_mask;

USE ROLE ADMIN;

SELECT * FROM employee_internal_stage LIMIT 10;

SELECT * FROM employee_external_stage LIMIT 10;

USE ROLE ACCOUNTADMIN;

-- Granting required previlages to role developer
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_internal_stage TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_external_stage TO ROLE PII;

USE ROLE PII;

SELECT * FROM employee_internal_stage LIMIT 10; 

SELECT * FROM employee_external_stage LIMIT 10;

USE ROLE ACCOUNTADMIN;

-- Granting required previlages to role developer
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_internal_stage TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_external_stage TO ROLE DEVELOPER;

USE ROLE DEVELOPER; -- using the role Developer

SELECT * FROM employee_internal_stage LIMIT 10; 

SELECT * FROM employee_external_stage LIMIT 10;
