## TSEU NIKITA
**201 Big Data Mentoring Program Global 2021** 
**_Homework 2_** – HDFS

 1. **Put datasets in HDFS system**
  This can be done in a fairly simple way - just with ```hdfs dfs -put <from> <to>``` command. Below you can see some screenshots which proof files presence in HDFS.
  
 ![Datasets in HDFS](images/hdfs_datasets.jpg)
 
 2. **Receive schema from Avro files**
 It is convenient to use Avro Tools, which provides us with a CLI interface to get a schema from Avro files.
 
 ![Avro schema](images/avro_schema.jpg)
 
  3. **Receive schema from Parquet files**
 This task is done in the same way as the previous one but with Parquet Tools.
 
 ![Parquet schema](images/parquet_schema.jpg)
 
4. **Collect rows count in Avro dataset using shell scripting and Avro Tools**
Below you can see the code of the script I wrote and the result of its work.

```bash
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
```
 ![Records count](images/avro_records.jpg)
