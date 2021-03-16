## TSEU NIKITA
**201 Big Data Mentoring Program Global 2021** 
**_Homework 4_** – Kafka

### Part 1 - Clickstream Data Analysis Pipeline Using KSQL

This tutorial shows how KSQL can be used to process a stream of click data, aggregate and filter it, and join to information about the users. Visualisation of the results is provided by Grafana, on top of data streamed to Elasticsearch.

The schematic view of the data pipeline is following:

![pipeline schema](images/clickstream/schema.jpg)

 1. **Create the Clickstream Data with ksqlDB**
  After performing all steps described in the tutorial we get the following data flow in ksqlDB:<br/>
 ![data flow](images/clickstream/flow.jpg)<br/>
 Three source connectors push mock data into Kafka topics lying under the ksqlDB, then this data is stored in the "sourse" tables and streams (`CLICKSTREAM_CODES, WEB_USERS, CLICKSTREAM`) and the other tables and streams are derived from the "source" ones. 
 
 2. **Visualize created data in Grafana**
 Grafana provides us with a great tools to perform interactive visualization of input data streams. So in this tutorial six metrics are counted:
 - Total number of _4xx_ error codes
 - Number of clicks each user have done
 - Count of user activity for a given user session
 - Page views counter
 - Total evens counter
 - Counter for every particular response code<br/>
  ![metrics 1](images/clickstream/metrics_1.jpg)
  ![metrics 2](images/clickstream/metrics_2.jpg)
  ![metrics 3](images/clickstream/metrics_3.jpg)
  ![metrics 4](images/clickstream/metrics_4.jpg)
  ![metrics 5](images/clickstream/metrics_5.jpg)
  ![metrics 6](images/clickstream/metrics_6.jpg)
  
  3. **Sessionize the data**
One of the tables created in the tutorial, `CLICK_USER_SESSIONS`, shows a count of user activity for a given user session. All clicks from the user count towards the total user activity for the current session. If a user is inactive for 30 seconds, then any subsequent click activity is counted towards a new session.<br/>
The clickstream demo simulates user sessions with a script. The script pauses the `DATAGEN_CLICKSTREAM` connector every 90 seconds for a 35 second period of inactivity. By stopping the `DATAGEN_CLICKSTREAM` connector for some time greater than 30 seconds, we will get distinct user session.<br/>
![grafana sessionized](images/clickstream/grafana_sessioned.jpg)<br/>

### Part 2 - Joining hotels with weather
 __Input__:
- Dataset with hotels (already enriched with geohashes)
- Dataset with weather records

__Task__:
- Load input data into the Hive tables.
 - Perform cross join of hotels with date dimension (same period as in weather data).
 - Load hotels and weather records into the Kafka topics using Hive-Kafka integration.
 - Create a Kafka Streams application which will:
   -  Enrich weather records with geohash.
   - Join hotels with weather by date and geohash.  In case of multiple results for a hotel in a particular day they should be grouped in a single entity, calculating average weather parameters. 
   - Write output to separate Kafka topic.

__Operating with data in Hive__:
- Load weather data from HDFS to Hive table with corresponding partitioning : 
```sql
create external table weather_storage 
		(lng double, lat double, avg_tmpr_f double, avg_tmpr_c double, wthr_date string) 
	partitioned by 
		(year integer, month integer, day integer) 
	stored as parquet 
	location '/datasets/kafka/weather';
```
- Create a special table connected with Kafka topic:
```sql
create external table weather 
		(lng double, lat double, avg_tmpr_f double, avg_tmpr_c double, wthr_date string) 
	stored by 
		'org.apache.hadoop.hive.kafka.KafkaStorageHandler' 
	tblproperties 
		("kafka.topic"="weather", "kafka.bootstrap.servers"="localhost:9092");
```
- Load data from the intermediate table to the table connected with Kafka topic (+sorting by date because we are trying to model data stream):
```sql
insert into table weather (
	select 
		`lng`, `lat`, `avg_tmpr_f`, `avg_tmpr_c`, `wthr_date`, 
		 null as `__key`, null as `__partition`, -1 as `__offset`, -1 as `__timestamp` 
	from 
		weather_storage 
	order by 
		`wthr_date`
	);
```
__Kafka Streams application__:

To be honest - this task seemed not too difficult, but it was a really tough nut to crack.

One of the problems is that in Kafka Strems left join produces additional _(value, null)_ pair for every record from left stream. Obviously, we don't need this pairs in the output, but we can't just filter them out - in this case we will get the same result as with inner join. Actually, I have tried different approaches to get rid of redundant records - but nothing have helped. So, finally, I've gave up and used just inner join.

Also there were a lot of other pitfalls like suppressing aggregation windows, dealing with data deletion because of Kafka retention policy, setting up grace period for join windows, etc.

As a result we have a topic with 214605 messages - it means that mapping with weather has been built for 2333 out of 2494 hotels.

![application output](images/kafka-streams-out.jpg)<br/>

The source code of the application is published on [GitHub](https://github.com/NikitaTseu/epam-big-data-training/tree/main/kafka-streams).
