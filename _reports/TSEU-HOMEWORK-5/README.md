## TSEU NIKITA
**201 Big Data Mentoring Program Global 2021** 
**_Homework 5_** – Hive

### Part 1 - Preparations

 For this task we will use __expedia dataset__ and __hotels dataset__. 

The __expedia dataset__ contains information about hotel visits (stored as avro files on HDFS), and the __hotels dataset__ is the joined hotel/weather dataset from the previous homework stored in Kafka topic. 

So now lets perform some preparations to simplify the work ahead.
First of all we need to store our data in some Hive tables.
```sql
CREATE EXTERNAL TABLE hotels_from_kafka 
		(avgTemperatureF double, avgTemperatureC double, id string, 
		name string, country string, city string, latitude string, 
		longitude string, geohash string, `date` string) 
	STORED BY 
		'org.apache.hadoop.hive.kafka.KafkaStorageHandler' 
	TBLPROPERTIES (
		"kafka.topic"="hotels-with-weather", 
		"kafka.bootstrap.servers"="localhost:9092");
```

```sql
CREATE EXTERNAL TABLE visits_from_hdfs 
	ROW FORMAT SERDE 
		'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
	STORED AS 
	INPUTFORMAT 
		'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
	OUTPUTFORMAT 
		'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' 
	LOCATION 
		'/datasets/expedia' 
	TBLPROPERTIES (
		'avro.schema.url'='/datasets/expedia_schema.avsc');
```

Lets take a look at resulting tables structure using hive command `describe`:

```sql
avgtemperaturef         	double
avgtemperaturec         	double 
id                      	string
name                    	string
country                 	string
city                    	string
latitude                	string
longitude               	string
geohash                 	string
date                    	string
__key                   	binary 
__partition             	int    
__offset                	bigint
__timestamp             	bigint
```

```sql
id                      	bigint
date_time               	string
site_name               	int
posa_continent          	int
user_location_country   	int
user_location_region    	int
user_location_city      	int
orig_destination_distance   double
user_id                 	int
is_mobile               	int
is_package              	int
channel                 	int
srch_ci                 	string
srch_co                 	string
srch_adults_cnt         	int
srch_children_cnt       	int
srch_rm_cnt             	int
srch_destination_id     	int
srch_destination_type_id    int
hotel_id                	bigint
```
<br/>This format isn't very convenient for the further work, so some additional transformations are required. 
- For __hotels__ we will create a new table partitioned by year and month with separate columns for day/month/year.

```sql
CREATE EXTERNAL TABLE hotels (
		id string, name string, country string, city string, lat string, lng string, 
		geohash string, date_full string, date_day int, tmpr_f double, tmpr_c double)
	PARTITIONED BY 
		(date_year int, date_month int);
```
```sql
INSERT INTO TABLE hotels 
	PARTITION (
		date_year, date_month)
	SELECT 
		id, name, country, city, latitude, longitude, geohash, `date`, 
		day(`date`), avgtemperaturef, avgtemperaturec, year(`date`), month(`date`) 
	FROM 
		hotels_from_kafka; 
```
- For __visits__ we will create a new table with separate columns for check-in and check-out day/month/year and partitioning by user request submit year/month. Actually, I think that any partitioning will not be very useful for our specific requests, but the idea to divide visits by request submit year/month looks good in general so I just decided to train. <br/><br/>__!__ The weird thing about this dataset is that some records in it have check-in date later then check-out date. In such cases I swapped this dates in order to get only correct records.

```sql
CREATE EXTERNAL TABLE visits (
		id bigint, date_time string, site_name int, posa_continent int, 
		user_location_country int, user_location_region int, user_location_city int, 
		orig_destination_distance double, user_id int, is_mobile int, is_package int, 
		channel int, srch_ci string, srch_co string, srch_adults_cnt int, srch_children_cnt int, 
		srch_rm_cnt int, srch_destination_id int, srch_destination_type_id int, hotel_id bigint,
		year_in int, month_in int, day_in int, year_out int, month_out int, day_out int)
	PARTITIONED BY 
		(sbm_year int, sbm_month int);
```

```sql
INSERT INTO TABLE visits 
	PARTITION (
		sbm_year, sbm_month
	)
	SELECT 
		id, date_time, site_name, posa_continent, user_location_region,
		user_location_country, user_location_city, orig_destination_distance, 
		user_id, is_mobile, is_package, channel, srch_ci, srch_co,
		srch_adults_cnt, srch_children_cnt, srch_rm_cnt, srch_destination_id, 
		srch_destination_type_id, hotel_id, year(srch_ci), month(srch_ci),
		day(srch_ci), year(srch_co), month(srch_co), day(srch_co), 
		year(date_time), month(date_time) 
	FROM 
		visits_from_hdfs
	WHERE 
		srch_ci < srch_co;

INSERT INTO TABLE visits 
	PARTITION (
		sbm_year, sbm_month
	)
	SELECT 
		id, date_time, site_name, posa_continent, user_location_region,
		user_location_country, user_location_city, orig_destination_distance, 
		user_id, is_mobile, is_package, channel, srch_co, srch_ci,
		srch_adults_cnt, srch_children_cnt, srch_rm_cnt, srch_destination_id, 
		srch_destination_type_id, hotel_id, year(srch_co), month(srch_co),
		day(srch_co), year(srch_ci), month(srch_ci), day(srch_ci), 
		year(date_time), month(date_time) 
	FROM 
		visits_from_hdfs
	WHERE 
		srch_ci > srch_co;
```

- Also it will be very useful to create a separate table with the __date dimension__, which will contain something like a calendar with any information we want to know about the date/time.

```sql
set hivevar:start_day=2016-01-01;
set hivevar:end_day=2018-12-31;
 
create table if not exists date_dim as
with dates as (
	select 
		date_add("${start_day}", arr.pos) as base_date
	from (select posexplode(
		split(repeat("x", datediff("${end_day}", "${start_day}")), "x"))) arr
)
select
    base_date as base_date, 
	year(base_date) as year, 
	month(base_date) as month, 
	day(base_date) as day, 
	date_format(base_date, 'EEEE') as dayname_of_week
from 
	dates
sort by 
	base_date;
```
Resulting table will look something like this:
```sql
   DATE         YEAR  MONTH    DAY     DAY_OF_WEEK
2016-01-01      2016    1       1       Friday
2016-01-02      2016    1       2       Saturday
2016-01-03      2016    1       3       Sunday
2016-01-04      2016    1       4       Monday
2016-01-05      2016    1       5       Tuesday
2016-01-06      2016    1       6       Wednesday
2016-01-07      2016    1       7       Thursday
2016-01-08      2016    1       8       Friday
...
...
2018-12-30      2018    12      30      Sunday
2018-12-31      2018    12      31      Monday
```

### Part 2 - Queries

1. **Top 10 hotels with max absolute temperature difference by month**
```sql
with
hotels_diff as (
	select 
		id, date_year, date_month, (max(tmpr_c) - min(tmpr_c)) as tmpr_diff 
	from 
		hotels 
	group by 
		id, date_year, date_month
),
result as (
	select
		id, date_year, date_month, tmpr_diff, 
		rank() over (partition by date_month order by tmpr_diff desc) as rank
	from 
		hotels_diff
)
select
	id, date_year, date_month, tmpr_diff
from 
	result
where 
	rank <= 10;
```
The main idea of this query is following - first we get __hotels_diff__ temporary table which contains temperature differences for every hotel in dataset by month. Now we need to choose top 10 hotels for _every_ month. This goal can be achieved by using partitioning by month and the `rank()` function. This subquery is also designed as CTE because we need to use `where` statement with the _rank_ column.

 After query execution we get the following result - top 10 hotels with their temperature diffs for every of three monthes in the dataset.
```sql
  HOTEL_ID      YEAR  MONTH      TEMPERATURE_DIFF
446676598786    2017    8       15.45
1005022347266   2017    8       15.2
541165879300    2017    8       14.283999999999995
1425929142272   2017    8       13.912500000000001
1013612281866   2017    8       13.588888888888892
919123001347    2017    8       13.545833333333336
824633720832    2017    8       13.399999999999999
953482739712    2017    8       13.266666666666662
996432412677    2017    8       13.247826086956525
42949672961     2017    8       13.247826086956525

1571958030336   2017    9       22.424999999999997
996432412673    2017    9       22.34347826086957
549755813890    2017    9       22.34347826086957
403726925824    2017    9       22.34347826086957
231928233987    2017    9       21.992000000000004
1606317768705   2017    9       21.992000000000004
996432412678    2017    9       21.752000000000002
970662608897    2017    9       21.752000000000002
730144440321    2017    9       21.752000000000002
755914244102    2017    9       21.262499999999992

1468878815239   2016    10      21.641666666666662
317827579908    2016    10      21.641666666666662
1065151889409   2016    10      21.287499999999998
1365799600129   2016    10      21.095833333333335
747324309508    2016    10      20.71818181818182
77309411328     2016    10      20.644
1400159338496   2016    10      20.315999999999995
893353197569    2016    10      19.791666666666668
1477468749828   2016    10      19.766666666666662
77309411330     2016    10      19.699999999999996
```
Also I conducted a small experiment - I created two similar tables with hotels, but they additionally were divided into 10 and 30 buckets by the hotel id. Total time to perform the query total working time was left approximately the same, so I concluded that on such a small dataset, bucketing is not very efficient.

2. **Top 10 busy (with the biggest visits count) hotels for each month. If visit dates refer to several months it should be counted for all affected months.**

```sql
with
visit_periods as (
	select 
		hotel_id, month_in, year_in, month_out, year_out, count(*) as cnt
	from 
		visits 
	group by 
		hotel_id, month_in, year_in, month_out, year_out
),
dtd as (
	select year, month from date_dim group by year, month
),
count_total as (
	select 
		visit_periods.hotel_id as id, dtd.month as date_month, 
		dtd.year as date_year, sum(visit_periods.cnt) as total
	from 
		dtd join visit_periods on (
			visit_periods.month_in <= dtd.month 
			and visit_periods.year_in <= dtd.year
			and visit_periods.month_out >= dtd.month
			and visit_periods.year_out >= dtd.year)
	group by 
		dtd.month, dtd.year, visit_periods.hotel_id
),
result as (
	select
		id, date_year, date_month, total, 
		row_number() over (partition by date_year, date_month order by total desc) as rank
	from
		count_total
)
select
	id, date_year, date_month, total, rank
from 
	result
where 
	rank <= 10;
```
Let understand this script by going through its parts:

- In the first CTE for every hotel we get all unique pairs of check-in year/month and check-out year/month and calculate total numbers of visits which has corresponding check-in/out dates _(for example if we have pair "09/2020" and "10/2020" we will count all visits with check-in in September 2020 and check-out in October 2020 for every particular hotel)_.

 - Then we just group our date dimension in order to get year/month combinations, because in this task we don't need days - they will spoil our join in the next CTE.

- Now the main part - _count_total_ table. In this CTE we take every year/month from date dimension and find all records from _visit_periods_ where check-in date is before our current taken date and check-out date is after it. It means that all visits from this period should be counted for the taken year/month. Also I use `row_number()` instead of `rank()` in order to get _exactly_ 10 records for every month in resulting table.

Here's the piece of resulting table (full result is stored in `res02.txt` file in archive with this homework). Full result may seem strange, but it's because of extremely unbalanced input dataset (I performed the small EDA to see check-in and check-out statistics by month, results are in the `eda.txt` file). 
```sql
  HOTEL_ID      YEAR  MONTH    VISITS  RANK
2491081031684   2017    8       408     1
42949672963     2017    8       403     2
128849018881    2017    8       393     3
2233382993923   2017    8       392     4
2619930050560   2017    8       392     5
1864015806471   2017    8       392     6
377957122048    2017    8       392     7
2611340115970   2017    8       391     8
146028888072    2017    8       390     9
3307124817921   2017    8       390     10

455266533377    2017    9       445     1
2018634629125   2017    9       432     2
893353197572    2017    9       421     3
1881195675654   2017    9       418     4
1829656068103   2017    9       418     5
2920577761284   2017    9       417     6
128849018887    2017    9       414     7
3204045602824   2017    9       414     8
3032246910980   2017    9       413     9
1494648619010   2017    9       412     10

3186865733636   2017    10      57      1
2628519985161   2017    10      51      2
1013612281859   2017    10      51      3
2877628088322   2017    10      50      4
1460288880640   2017    10      50      5
910533066756    2017    10      50      6
1331439861761   2017    10      50      7
1675037245442   2017    10      49      8
893353197570    2017    10      49      9
1314259992576   2017    10      49      10
```
<br/>

3. **For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.**

```sql
with 
long_visits as (
	select * from visits where datediff(srch_co, srch_ci) > 7
),
long_visits_enriched as (
	select 
		long_visits.id as visit_id, 
		sort_array(collect_list(
			concat_ws('x', hotels.date_full, cast(hotels.tmpr_c as string)))) as weather, 
		avg(hotels.tmpr_c) as avg_tmpr
	from 
		long_visits join hotels on (
			    hotels.id = long_visits.hotel_id
			and hotels.date_full >= long_visits.srch_ci
			and hotels.date_full <= long_visits.srch_co
		)
	group by
		long_visits.id
)
select 
	visit_id, 
	cast(avg_tmpr as decimal(12, 2)), 
	cast(split(weather[size(weather)-1], 'x')[1] as decimal(12, 2)) 
		- cast(split(weather[0], 'x')[1] as decimal(12, 2)) as tmpr_diff
from 
	long_visits_enriched 
limit 10;
```

The idea of this query is pretty simple, but some manipulations can be not clear, so I'll try to explain them.
First let's take a look on this statement. `collect_list` operator can be used instead of aggregation with `group by` in order to get the list of grouped records for every group. Here for every visit we get list of strings in format `<date>x<temperature>` for every day of this visit. Then this list is sorted and we have element matching the first day of visit on the first place in the array and element matching the last day - on the last place.
```sql
sort_array(collect_list(concat_ws('x', hotels.date_full, cast(hotels.tmpr_c as string))))
```
Then we take the last and the first elements from the array, extract temperatures by splitting string and calculate the temperature difference between last and first day of stay.
```sql
cast(split(weather[size(weather)-1], 'x')[1] as decimal(12, 2)) -
cast(split(weather[0], 'x')[1] as decimal(12, 2))
```
Also this query could be written in more understandable way using window functions:
```sql
with 
long_visits as (
	select * from visits where datediff(srch_co, srch_ci) > 7
)
select 
	long_visits.id as visit_id, 
	avg(hotels.tmpr_c) over (partition by long_visits.id) as avg_tmpr,
	last_value(hotels.tmpr_c) over (partition by long_visits.id order by hotels.date_full) -
		first_value(hotels.tmpr_c) over (partition by long_visits.id order by hotels.date_full)	
	as tmpr_diff
from 
	long_visits join hotels on (
		    hotels.id = long_visits.hotel_id
		and hotels.date_full >= long_visits.srch_ci
		and hotels.date_full <= long_visits.srch_co
	)
limit 10;
```
<br/>

__Interesting note:__ 
It was a big discovery for me, but `avg(hotels.tmpr_c)  over  (partition  by long_visits.id)` and  `avg(hotels.tmpr_c)  over  (partition  by long_visits.id order by hotels.tmpr_c)` will return completely different results. Why it's happen you can read for example [here](https://www.postgresql.org/docs/9.1/tutorial-window.html).

<br/>
Results (I set up 10 records limit because amount of extended visits is quite large):

```sql
 VISIT_ID     AVG_TMPR    TMPR_TREND
	17  	    21.58   	-0.83
	45      	15.32   	-2.70
	96    	    12.63   	 5.15
	136     	22.78   	 1.85
	350         16.73   	-1.50
	364     	12.64   	 3.70
	408     	15.90   	 1.05
	475     	16.15   	-1.28
	508     	14.42   	-7.00
	512     	16.21   	-0.12
```
### Part 3 - Execution plans

To optimise joins in my scripts I put a bigger table into the right side of join because in this case it isn't stored in memory - instead of this it is handled as a stream. Also a relatively simple way to improve the performance is to divide tables into the buckets by the columns used in join.

Execution plans are stored in the archive with this homework in files named `select*expl.txt` with the number of query instead of `*`. 

Execution plans for the first and the third queries didn't have any warnings and execution plan for the second query had the following warnings:
```
Warning: Map Join MAPJOIN[60][bigTable=?] in task 'Stage-7:MAPRED' is a cross product
Warning: Map Join MAPJOIN[50][bigTable=?] in task 'Stage-6:MAPRED' is a cross product
Warning: Shuffle Join JOIN[14][tables = [$hdt$_0, $hdt$_1]] in Stage 'Stage-2:MAPRED' is a cross product
```
This query has only one join and by design I don't think that it can be avoided or optimised in some way, so I just left it as it is.
