# vector_field_framework

This framework consists of three parts： data producer, parallel computing and services.<br>

1.data producer<br>
This part runs on the data source as a data producer, configures the segmentation algorithm according to the data segmentation, and configures the cluster address and the Kafka topic information simultaneously.<br>
Enviroment:jvm1.8<br>

2.parallel computing<br>
Running on the cluster side as a spark-streaming application, you need to configure the consumption theme information and add algorithm based on the actual calculation.<br>
Enviroment:spark-2.1.0<br>

3.services<br>
In the data service part, the dataflow gate is designed to control data output.Real time data services and historical data services need to design service interfaces separately.<br>
Enviroment:jvm1.8<br>
