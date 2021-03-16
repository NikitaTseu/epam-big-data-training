#!/bin/sh

# This script counts the number of records in avro files
# in a specific HDFS directory (passed as command line argument)

RECORDS_TOTAL=0
RECORDS_IN_FILE=0
FILE_COUNTER=0

for ENTRY in `hdfs dfs -ls "$1"`
do
  if [ "${ENTRY: -5}" == ".avro" ]; then
    RECORDS_IN_FILE=`hadoop jar ~/avro/avro-tools-1.10.1.jar count "$ENTRY"`
    RECORDS_TOTAL=`expr $RECORDS_TOTAL + $RECORDS_IN_FILE`
	
	FILE_COUNTER=`expr $FILE_COUNTER + 1`
	echo "$FILE_COUNTER" files processed...
  fi
done

echo Avro files in the specified directory contain "$RECORDS_TOTAL" records
