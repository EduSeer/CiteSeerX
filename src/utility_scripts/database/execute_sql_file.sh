#!/bin/bash

# TODO extract from properties file
mysql_user='csx-devel'
mysql_pw='csx-devel'

mysql --user=$mysql_user --password=$mysql_pw -h localhost -P 3306 < $1
