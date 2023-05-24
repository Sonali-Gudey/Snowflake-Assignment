## Ankit and Pranneth's Approach

1. Roles are created using create role command. Created a hierarchy between them as per requirements.
2. From account admin role data warehouse is created.
3. Switched to admin role.
4. Given some privileges to the admin role and database is created.
5. Schema is created as mentioned in question.
6. Employee data table is created according to the schema of the csv file.
7. Integration with aws, file format, and external staging are created.
8. Variant is created. For loading data an internal stage is created and data is loaded into a variant table using insert into commands.
9. Internal stage is created. CSV file is loaded into the created table from local by running put command in command prompt.
10. By using copy command data is loaded into internal and external tables.
11. Some query commands are runned on the above table.
12. Created a parquet file format using put command from command prompt data is loaded.
13. For getting infer_schema query commands are run on parquet stage.
14. Masking policies are created and applied on columns of tables.
15. Different privileges are given to pii and developer roles.
16. Query commands are run on tables from different roles.


## Comparision:

