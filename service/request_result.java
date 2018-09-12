package service;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import javax.websocket.Session;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class search implements Runnable{
    Session session;
    search(Session csession){
        this.session=csession;
    }
    public void run(){
        //kafka配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.200:9092,192.168.1.201:9092,192.168.1.202:9092");
        props.put("group.id", "hello-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("result"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            }
        });
        HashMap<Long,Integer> container=new HashMap<Long,Integer>();
        try {
            while (true) {
                    ConsumerRecords<Integer, String> records = consumer.poll(500);
                    for (ConsumerRecord<Integer, String> record : records) {
                        //   session.getBasicRemote().sendText(record.value());
                        long ph=Long.parseLong(record.value())/100;
                        if(container.containsKey(ph))
                        {
                            if(container.get(ph)==3)
                            {
                                this.session.getBasicRemote().sendText(String.valueOf(ph));
                                container.remove(ph);
                            }
                            else
                                container.put(ph,container.get(ph)+1);

                        }
                        else
                            container.put(ph,1);
                    }

                    TimeUnit.MICROSECONDS.sleep(500);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }catch (IOException e2)
        {
            e2.printStackTrace();
        }
        finally {
            consumer.close();
        }
    }
}
