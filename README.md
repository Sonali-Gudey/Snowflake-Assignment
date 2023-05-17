# Snowflake-Assignment

### Question-1: Create roles as per the below-mentioned hierarchy. Accountadmin  already exists in Snowflake.

![image](https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/1cc1b328-37e1-4d62-ad64-785ca5a5f6f6)

#### Approach:
```
CREATE ROLE DEVELOPER;
CREATE ROLE ADMIN;
CREATE ROLE PII;

GRANT ROLE DEVELOPER TO ROLE ADMIN;
GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;
```


<img width="1027" alt="Screenshot 2023-04-12 at 4 37 31 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/a4fe0516-6ad0-47ba-9e2b-69e3c4460932">

### Question - 2: Create an M-sized warehouse using the accountadmin role, name -> assignment_wh and use it for all the queries.

#### Approach:

```
CREATE OR REPLACE WAREHOUSE assignment_wh
WITH WAREHOUSE_SIZE = 'MEDIUM';
```

#### Granting privileges:

```
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE ADMIN;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE ADMIN;
```

### Question - 3: Switch to the admin role

```
USE ROLE ADMIN;
```
### Question - 4: Create a database assignment_db

```
CREATE DATABASE ASSIGNMENT_DB;
```

### Question - 5: Create a schema my_schema

```
CREATE SCHEMA MY_SCHEMA;
```

### Question - 6: Create a table using any sample csv. You can get 1 by googling for sample csv’s. Preferably search for a sample employee dataset so that you have PII related columns else you can consider any column as PII .

#### Approach:

```
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
```

### Question - 7: Also, create a variant version of this dataset.

```
CREATE OR REPLACE FILE FORMAT my_json_format TYPE = JSON;
```

```
CREATE TABLE variant_table (                     
    variant_data variant
);
```

```
put file:///Users/sonali_gudey/Downloads/employee.json @%variant_table;
```


<img width="967" alt="Screenshot 2023-04-12 at 6 53 13 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/a2753b27-9aa8-4861-aecc-7ffb94728910">

```
COPY INTO variant_table 
FROM @%variant_table file_format = MY_JSON_FORMAT;
```

<img width="1119" alt="Screenshot 2023-04-13 at 11 10 39 AM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/f4cd816d-8907-4363-8d37-af0e571184a2">

```
select * from variant_table;
```

<img width="1109" alt="Screenshot 2023-04-13 at 2 41 35 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/14f2ad46-2231-42fe-a485-3a65b9a4b415">


### Question - 8: Load the file into an external and internal stage.

#### Approach:


##### Steps for loading data to internal_stage

```
CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT             
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;
```

```
CREATE STAGE internal_stage file_format = my_csv_format;
```

```
put file:///Users/sonali_gudey/Downloads/employee.csv @internal_stage;
```


<img width="1006" alt="Screenshot 2023-04-13 at 11 49 08 AM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/3bd7caa6-13c4-44a8-ab8f-da5a51493c9c">


##### Steps for loading data into external_stage:

```
CREATE STORAGE INTEGRATION s3_integration
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::547795514214:role/sonali_role'
storage_allowed_locations = ('s3://snowflake-assignment001/csv_files/employee.csv');
```

```
GRANT ALL ON INTEGRATION s3_integration TO ROLE admin; 
```

```
DESC INTEGRATION s3_integration;
```

<img width="1109" alt="Screenshot 2023-04-13 at 12 39 22 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/7125619f-316b-40cc-b069-14e5334fc7c7">


```
CREATE OR REPLACE STAGE external_stage
URL = 's3://snowflake-assignment001/csv_files/employee.csv'
STORAGE_INTEGRATION = s3_integration
FILE_FORMAT = my_csv_format;
```

```
LIST @internal_stage;
```

<img width="1109" alt="Screenshot 2023-04-13 at 12 41 57 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/ccd1fd88-23b7-4b8c-96c9-3425b3e59f97">


```
LIST @external_stage;
```

<img width="1109" alt="Screenshot 2023-04-13 at 1 04 41 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/2bb052cc-5aa2-4706-9c95-f0893e08d4af">


### Question - 9: Load data into the tables using copy into statements. In one table load from the internal stage and in another from the external

Approach:

```
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
```

```
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
```

```
COPY INTO employee_internal_stage(id, first_name, last_name, email, age, salary, phone_number,file_name)
FROM ( 
SELECT emp.$1, emp.$2, emp.$3, emp.$4, emp.$5, emp.$6, emp.$7, METADATA$FILENAME 
FROM @internal_stage/employee.csv.gz (file_format => my_csv_format) emp);
```

```
COPY INTO employee_external_stage(id, first_name, last_name, email, age, salary, phone_number,file_name)
FROM ( 
SELECT emp.$1, emp.$2, emp.$3, emp.$4, emp.$5, emp.$6, emp.$7, METADATA$FILENAME 
FROM @external_stage (file_format => my_csv_format) emp);
```

```
select * from employee_internal_stage limit 10;
```

<img width="1109" alt="Screenshot 2023-04-13 at 2 41 02 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/16f884de-6208-4db6-8b15-2f9af8e89506">


```
select * from employee_external_stage limit 10;
```

<img width="1109" alt="Screenshot 2023-04-13 at 2 41 35 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/e96ac135-863d-4bf5-92eb-6e9ef01917e4">


### Question - 10: Upload any parquet file to the stage location and infer the schema of the file.

Approach:


```
CREATE FILE FORMAT my_parquet_format TYPE = parquet;
```

```
CREATE STAGE parquet_stage file_format = my_parquet_format;
```

```
put file:///Users/sonali_gudey/Downloads/employee.parquet @parquet_stage;
```

<img width="996" alt="Screenshot 2023-04-13 at 2 45 11 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/7578b427-bc72-4f0e-b512-2de63f6fd3e8">


```
SELECT * FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@parquet_stage',
      FILE_FORMAT=>'my_parquet_format'
    )
);
```

<img width="1114" alt="Screenshot 2023-04-13 at 2 46 19 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/7bd99193-9e82-4676-8fd4-79bd259c6660">


### Question - 11: Run a select query on the staged parquet file without loading it to a snowflake table.

Approach:

```
SELECT * from @parquet_stage/employee.parquet;
```

<img width="1182" alt="Screenshot 2023-04-13 at 2 47 54 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/8ac83142-06ff-4e4e-a25d-7df791ee0551">


### Question - 12
Add masking policy to the PII columns such that fields like email, phone number, etc. show as masked to a user with the developer role. If the role is PII the value of these columns should be visible.

Approach:


--email_mask

```
CREATE OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string ->
CASE
WHEN CURRENT_ROLE() = 'PII' THEN VAL
ELSE '****MASK****'
END;
```


--contact_mask

```
CREATE OR REPLACE MASKING POLICY contact_mask AS (VAL string) RETURNS string -> 
CASE
WHEN CURRENT_ROLE() = 'PII' THEN VAL
ELSE '****MASK****'
END;
```

```
-- Adding the email mask policy to internal stage
ALTER TABLE IF EXISTS employee_internal_stage 
MODIFY EMAIL SET MASKING POLICY email_mask;
```

```
-- Adding the email mask policy to external stage
ALTER TABLE IF EXISTS employee_external_stage 
MODIFY EMAIL SET MASKING POLICY email_mask;
```

```
-- Adding the contact mask policy to internal stage
ALTER TABLE IF EXISTS employee_internal_stage 
MODIFY phone_number SET MASKING POLICY contact_mask;
```

```
-- Adding the contact mask policy to external stage
ALTER TABLE IF EXISTS employee_external_stage 
MODIFY phone_number SET MASKING POLICY contact_mask;
```

```
USE ROLE ADMIN;
```

```
SELECT * FROM employee_internal_stage LIMIT 10;
```

<img width="1182" alt="Screenshot 2023-04-13 at 3 15 32 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/666af4e1-c78e-4d5d-97b6-d943483a0e88">


```
SELECT * FROM employee_external_stage LIMIT 10;

```

<img width="1182" alt="Screenshot 2023-04-13 at 3 15 45 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/d14e753d-2ea0-40c9-a885-5566899ffb23">

* PII role
As we need to display data from PII view, we need to grant certain previlages to PII role, for this we need to switch to ACCOUNTADMIN role.

```
USE ROLE ACCOUNTADMIN;

-- Granting required previlages to role developer
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_internal_stage TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_external_stage TO ROLE PII;

USE ROLE PII;
```


```
SELECT * FROM employee_internal_stage LIMIT 10; 
```

<img width="1182" alt="Screenshot 2023-04-13 at 3 13 54 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/f8d6e0ae-b8a9-496e-8fb3-6ee16986ce41">


```
SELECT * FROM employee_external_stage LIMIT 10;
```

<img width="1182" alt="Screenshot 2023-04-13 at 3 14 03 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/31a2298a-dfdd-43a1-b1f5-718eeaef631a">


* Developer role
As we need to display data from Developer view, we need to grant certain previlages to Developer role, for this we need to switch to ACCOUNTADMIN role.

```
USE ROLE ACCOUNTADMIN;

-- Granting required previlages to role developer
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_internal_stage TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_external_stage TO ROLE DEVELOPER;

USE ROLE DEVELOPER; -- using the role Developer
```

```
SELECT * FROM employee_internal_stage LIMIT 10; 
```

<img width="1182" alt="Screenshot 2023-04-13 at 3 15 32 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/144703d7-0b4a-4d80-a1b1-777d3a34e48f">


```
SELECT * FROM employee_external_stage LIMIT 10;
```

<img width="1182" alt="Screenshot 2023-04-13 at 3 15 45 PM" src="https://github.com/Sonali-Gudey/Snowflake-Assignment/assets/123619701/fbce0840-bbf1-40d5-b20f-0c78c8247dad">


















