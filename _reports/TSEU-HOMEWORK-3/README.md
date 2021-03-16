

## TSEU NIKITA
**201 Big Data Mentoring Program Global 2021** 
**_Homework 3_** – Data Flow, Pipelining

### Part 1 - Apache NiFi

 1. **Deploy NiFi with Kubernetes**
  This step was successfully performed according to given instructions. Here's a screenshot of working docker containers with NiFi cluster.
  
 ![docker containers](images/nifi/nifi_containers.jpg)
 
 2. **Complete HVAC tutorial**
 In this tutorial our goal is to build data pipeline, which will receive zip archive with data via HTTP, unpack it  and finally put files with data to HDFS according do their names. Screenshot of ready to work pipeline is provided below.
 
 ![Pipeline ready](images/nifi/pipeline_ready.jpg)

Actually, this tutorial assume that we have both HDFS and NiFi working on the same host, so they can easily communicate with each other. But in my case NiFi was deployed inside docker container, which provides us with independent environment, so I had to make some additional manipulations to establish the connection between NiFi and HDFS. 

>1. HDFS in my local computer is running on the `localhost:9000` according to the `core-site.xml` conf file. But the problem is that inside docker container we have its own loopback interface, so after copying  `core-site.xml` to the container we should change `localhost` in `core-site.xml` to the `host.docker.internal` address value (`192.168.65.2` in my case). `host.docker.internal` is the special address which is used to contact external services running on the host machine from the docker container.
>
>2. After receiving request from NiFi NameNode finds a DataNode which can be used for writing files, and this DataNode sends its IP address to NiFi. And here we face the same problem - in our case this address is private (`127.0.1.1`) so it is unreacheable from inside the docker container. According to Apache documentation in case of using multihomed networks we can configure DataNodes in such way that instead of sending their IP to clients, they now will send their hostname. 
>
>3. After changing DataNodes configuration we should go to the docker container with NiFi and modify `/etc/hosts` file to perform DNS resolution of the DataNode hostname. This hostname should be mapped to the `host.docker.internal` address to contact the DataNode from inside the container.

 
  3. **Final result**
 Here you can see the active pipeline and files stored in the HDFS.
 
 ![Pipeline active](images/nifi/pipeline_working.jpg)

 ![Files in HDFS](images/nifi/proof.jpg)

### Part 2 - StreamSets Data Collector

As the starting point in this task we have five `.zip` files with csv data, representing hotels addresses and coordinates. The main goal is to create a data pipeline, which will read input data from HDFS, fill missing coordinates based on hotel's address (if needed), then calculate geohash for every record and finally write output to kafka topic.

 1. **Pipeline schema**
So, the schema of the our pipeline will look like that:
![Pipeline schema](images/sdc/pipeline_general.jpg)
Here we hava `HDFS reader` as an origin - it unpacks input archives and parse data, creating separate records for every line in csv file. Then records go to the `Stream Selector` - this processor divides records into two streams depending on whether it is necessary to restore coordinates for input record. If coordinates are present, record immediately gets to the `Geohash Processor`, but if coordinates are missing, it first need to pass `Geocoding Processor`. `Geocoding Processor` is the custom processor written with Java API which can restore missing coordinates by known address or location. The only required parameter is API key for the OpenCage web service. 
![Geocoding API key](images/sdc/key.jpg)
The next stage in pipeline is `Geohash Processor` - like the previous one, this processor is custom and it's responsible for calculating geohash value by latitude and longitude. In setting you can set up geohash length (from 1 to 8 symbols).
![Geohash length](images/sdc/length.jpg)
Finally all records are written to kafka topic.
2. **Running the Pipeline**
Let's run our created Pipeline!
`HDFS reader` got almost 2494 records as input and 2494 records were written to kafka topic.
![Records count](images/sdc/records_general.jpg)
![Kafka output](images/sdc/kafka.jpg)
Also it's interesting to take a look at the examples of `Geocoding Processor` and `Geohash Processor` work.
![Geocoding example](images/sdc/geocoding_example.jpg)
![Geohash example](images/sdc/geohash_example.jpg)
3. **Source code for the custom processors**
Source code for the custom processors is published on [GitHub](https://github.com/NikitaTseu/epam-big-data-training/tree/main/geocoding). Also it's available as src jar  in my zipped report.
