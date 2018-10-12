# vector_field_framework

This framework consists of three parts： data producer, parallel computing and services.
1. data producer
This part runs on the data source, configures the segmentation algorithm according to the data segmentation, and configures the cluster address and the Kafka topic information simultaneously.
2.parallel computing
Running on the cluster side, you need to configure the consumption theme information and add algorithm based on the actual calculation.
3.services
In the data service part, the dataflow gate is designed to control data output.Real time data services and historical data services need to design service interfaces separately.
