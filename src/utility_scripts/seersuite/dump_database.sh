#!/bin/bash

date=$(date +%Y%m%d-%H%M%S)
target_file=~/eduseer_database_dump_${date}_on_host_$(hostname).sql.gz
user="csx-devel"
pw="csx-devel"
echo "
--------------------------------------------------------------------------------
DUMP CSX DATABASES
Dump file will go to $target_file
You may restore the database using:

mysql --user=csx-devel --password=csx-devel -e \"source sql_file.sql\"
--------------------------------------------------------------------------------
"

mysqldump --opt --user=$user --password=$pw --databases citeseerx csx_citegraph csx_external_metadata csxdoi myciteseerx | gzip -c > $target_file

