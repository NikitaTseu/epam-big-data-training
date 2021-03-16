## TSEU NIKITA
**201 Big Data Mentoring Program Global 2021** 
**_Homework 6.1_** – Spark batching

 
Task was the following:
-   Read hotels&weather data from Kafka with Spark application in a batch manner.
-   Read Expedia data from HDFS with Spark.
-   Calculate idle days (days betweeen current and previous check in dates) for every hotel.
-   Validate data:
    -   Remove all booking data for hotels with at least one "invalid" row (with idle days more than or equal to 2 and less than 30).
    -   Print hotels info (name, address, country etc) of "invalid" hotels and make a screenshot. Join expedia and hotel data for this purpose.
-   Group the remaining data and print bookings counts: 1) by hotel country, 2) by hotel city. Make screenshots of the outputs.
-   Store "valid" Expedia data in HDFS partitioned by year of "srch_ci" (check-in year).

<br/>

Here you can see results of my work:

Information about hotels with invalid booking data:
```
+-------------+--------------------+-------+--------------+
|           id|                name|country|          city|
+-------------+--------------------+-------+--------------+
|2662879723520|Okko Hotels Paris...|     FR|         Paris|
| 206158430210|  The Litchfield Inn|     US|Pawleys Island|
|3058016714753|           Hazlitt s|     GB|        London|
| 197568495617|Fairfield Inn & S...|     US|   East Peoria|
|2302102470656|Park Internationa...|     GB|        London|
|3100966387715|         Vincci Gala|     ES|     Barcelona|
+-------------+--------------------+-------+--------------+
```
<br/>

Bookings counts by hotel country:
```
+-------+-------+
|country|  count|
+-------+-------+
|     NL| 106063|
|     AT|   5016|
|     GB| 403247|
|     ES| 212520|
|     US|1004267|
|     FR| 463261|
|     IT| 164782|
+-------+-------+
```
<br/>

Bookings counts by hotel city (limit 10):
```
+------------------+-----+
|              city|count|
+------------------+-----+
|        Blythewood| 1017|
|        Prattville|  993|
|             Tyler| 1033|
|       Piney Creek|  995|
|     Bowling Green| 1949|
|        Harrisburg| 1028|
|       Springfield| 7094|
|           Whigham| 1046|
|          Harrison|  990|
|Indian Rocks Beach|  980|
+------------------+-----+
```
<br/>

Lets take a look at the content of the particular partition:

`hdfs dfs -ls /datasets/expedia_valid/ci_year=2016/`

Here we can see 10 parquet files because we configured 10 partitions in the SparkSession object in our application:
```
Found 10 items
-rw-r--r--   1 niktseu supergroup    3173369 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00000-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    3017568 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00001-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    2466322 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00002-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    3202546 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00003-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    2958458 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00004-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    2583942 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00005-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    2539164 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00006-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    3154115 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00007-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    2848992 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00008-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
-rw-r--r--   1 niktseu supergroup    2576318 2021-02-23 19:05 /datasets/expedia_valid/ci_year=2016/part-00009-336464ef-e88e-40d9-9843-4e3dff41b525.c000.snappy.parquet
```
<br/>

__Proof of runs both in local and cluster modes__:

I have added a special line of code to see what type of cluster manager is used (this parameter is provided when we submit jar with the application): 
```scala
println("CURRENT CLUSTER MANAGER IS ---> " + 
spark.sparkContext.getConf.get("spark.master", "DEFAULT"))
```


  ![local mode](images/local_mode_proof.jpg)
  
  ![yarn mode](images/yarn_mode_proof.jpg)

This code could be used to submit application (don't forget to change path to jar). I manually import some heavy dependencies in order to keep the uber jar lightweight. 
```
spark-submit --class com.epam.bigdata201.spark.App --master local[*] --packages org.apache.spark:spark-avro_2.12:3.0.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 /mnt/c/projects/epam-big-data-training/spark-batch/target/spark-batch-1.0-SNAPSHOT-jar-with-dependencies.jar
```

The source code of the application is published on [GitHub](https://github.com/NikitaTseu/epam-big-data-training/tree/main/spark-batch).
