# vector_field_framework

## prepare<br>
data source:jvm 1.8<br>
cluster enviroment:scala-2.11.0,zookeeper-3.3.4,hadoop-2.7.1,hbase-2.0，spark-2.1.0,kafka-0.10.2<br>
service enviroment:jvm 1.8,tomcat 7.0<br>

## config<br>
This framework consists of three parts： data producer, parallel computing and services.<br>

1.data producer<br>
This part runs on the data source as a data producer, configures the segmentation algorithm according to the data segmentation, and configures the cluster address and the Kafka topic information simultaneously.<br>

2.parallel computing<br>
Running on the cluster side as a spark-streaming application, you need to configure the consumption theme information and add algorithm based on the actual calculation.<br>

3.services<br>
In the data service part, the dataflow gate is designed to control data output.Real time data services and historical data services need to design service interfaces separately.<br>
