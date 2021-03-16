## TSEU NIKITA
**201 Big Data Mentoring Program Global 2021** 
**_Homework 1_** – Introduction to Big Data and Hadoop

### Part 1 - Ambari UI

 1. **Dashboard**
  Contains widgets from which we can get information about cluster state - resource usage, number of live DataNodes, cluster load and many others.
  Also Dashboard has a *"Heatmaps"* tab with metrics displayed in the heatmap form and *"Config history tab"*.
 ![Ambari dashboard](images/ambari/01_dashboard.jpg)
 
 2. **Services**
 Allows you to get quick information about the status of the services running on the cluster. More detailed information is available by clicking on the corresponding service name.
 ![Services](images/ambari/02_services.jpg)
 
  3. **Hosts**
 From this tab you can get detailed information about state and configuration of any node in the cluster.
 ![Hosts1](images/ambari/03_hosts1.jpg)
 ![Hosts2](images/ambari/03_hosts2.jpg)
 
4. **Alerts**
Gives opportunity to get and manage alerts for cluster services. Highly customizable tool.
 ![Alerts](images/ambari/04_alerts.jpg)

### Part 2 - Kafka
Kafka service is installed on cluster with Ambari:
![Kafka service is working](images/kafka/service_started.jpg)

So now we can create a topic and send some messages:
![Kafka messaging](images/kafka/kafka_messages.jpg)
