This folder contains utility SQL scripts:

 - drop_csx_databases.sql:
   removes the following databases: citeseerx, csx_citegraph, csx_external_metadata, myciteseerx, csxdoi
   
 - enable_all_users.sql:
   sets the enabled flag to 1 for all users.
   
 - publish_all_documents.sql:
   sets the public flag to 1 for all papers
   
The SQL scripts can be run with the help of the Bash script execute_sql_file.sh:

Example:
sh ./execute_sql_file.sh enable_all_users.sql
