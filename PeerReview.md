## Praneeth's Approach

1. Created roles in the hierarchy: Admin, Developer, and PII.
2. Granted the Developer role to the Admin role to establish the hierarchy.
3. Created an M-sized warehouse named "assignment_wh" using the accountadmin role.
4. Switched to the Admin role.
5. Created a database named "assignment_db".
6. Created a schema named "my_schema".
7. Created tables: "employee_local" and "employee_external" based on a sample CSV file.
8. Created a variant table named "employee_variant".
9. Loaded data into the tables using COPY INTO statements from internal and external stages.
10. Created an integration between Snowflake and Amazon S3 using an AWS IAM role.
11. Created an external stage and loaded data into it from an S3 bucket.
12. Uploaded a Parquet file to the internal stage and created a file format for it.
13. Ran a select query on the staged Parquet file without loading it into a table.
14. Added masking policy to PII columns (email and phone number) using a CASE statement.
15. Granted masking policy permissions to the Admin role.
16. Applied masking policies to the email and phone number columns of both internal and external tables.
17. Granted necessary privileges to the PII role and verified the masked values in the tables from the PII role.


## Ankit's Approach:

1. Created the required roles: DEVELOPER, ADMIN, and PII.
2. Granted roles in a hierarchical manner, ensuring that the roles follow the given hierarchy.
3. Created an M-sized warehouse named "assignment_wh" using the accountadmin role.
4. Switched to the admin role using the "USE ROLE" command.
5. Created a database named "assignment_db".
6. Created a schema named "my_schema".
7. Created a table using a sample CSV file, preferably an employee dataset, with columns containing Personally Identifiable Information (PII).
8. Created a variant version of the dataset by creating a table named "variant_table" with a column of type "variant".
9. Loaded the CSV file into both an external and an internal stage using the "PUT" command.
10. Loaded data from the stages into separate tables using the "COPY INTO" statements.
11. Uploaded a parquet file to the stage location and inferred the schema of the file.
12. Created a file format named "my_parquet_format" to handle the CSV format data.
13. Ran a select query on the staged parquet file without loading it into a Snowflake table.
14. Added a masking policy to the PII columns, such as email and phone number, to show as masked for users with the developer role. For users with the PII role, the original values of these columns should be visible.
15. Implemented the masking policies using a CASE statement that checks the current role and returns either the original value or a masked value.
16. Granted privileges to the roles to view the data, verifying that the masking was applied correctly based on the user's role.

## Comparision:

Both followed the same approaches by following a similar sequence of steps, starting with role creation, warehouse setup, database/schema creation,  loading data into tables, etc.

