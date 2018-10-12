# vector_field_framework

This framework consists of three parts： data producer, parallel computing and services.

1. data producer
This part runs on the data source as a data producer, configures the segmentation algorithm according to the data segmentation, and configures the cluster address and the Kafka topic information simultaneously.
Enviroment:jvm1.8

2.parallel computing
Running on the cluster side as a spark-streaming application, you need to configure the consumption theme information and add algorithm based on the actual calculation.
Enviroment:spark-2.1.0

3.services
In the data service part, the dataflow gate is designed to control data output.Real time data services and historical data services need to design service interfaces separately.
Enviroment:jvm1.8
