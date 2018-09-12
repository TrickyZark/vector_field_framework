package read;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import ucar.ma2.Array;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class sender implements Runnable{
        private Array aub;
        private Array avb;
        private String topic;
        private long i;
        private Producer<String, byte[]> procuder;
        private float[] float_p;
        private int[] int_p;
        sender(Array ub,Array vb,long x,String to,Producer<String, byte[]> pro,float[] fp,int[] ip){
            this.aub=ub;
            this.avb=vb;
            this.topic=to;
            this.i=x;
            this.procuder=pro;
            this.float_p=fp;
            this.int_p=ip;
        }
        public void run(){
          /*  byte[][] add = new byte[(int) this.aub.getSize() * 2][];
            for (int j = 0; j < this.aub.getSize(); j++) {
                add[j * 2] = Bytes.toBytes(this.aub.getFloat(j));
                add[j * 2 + 1] = Bytes.toBytes(this.avb.getFloat(j));
            }
            byte[] newb = Bytes.add(add);*/
            //String srt2 = new String(newb, "UTF-8");
            byte[] newb=new byte[(int) this.aub.getSize() * 2+40];
            byte[][] add = new byte[10][];
            for (int j = 0; j < 8; j++) {
                add[j] = Bytes.toBytes(float_p[j]);
            }
            add[8]= Bytes.toBytes(int_p[0]);
            add[9]= Bytes.toBytes(int_p[1]);
            byte[] newa = Bytes.add(add);
            float max_u=0;
            float max_v=0;
            float min_u=0;
            float min_v=0;
            System.arraycopy(newa,0,newb,0,40);
            for (int j = 0; j < this.aub.getSize(); j++) {
                newb[j * 2+40] = (byte) ((int)this.aub.getFloat(j));
                newb[j * 2 + 41] =(byte) ((int) this.avb.getFloat(j));
                if(max_u<this.aub.getFloat(j))
                {
                    max_u=this.aub.getFloat(j);
                }
                if(max_v<this.avb.getFloat(j))
                {
                    max_v=this.avb.getFloat(j);
                }
                if(min_u>this.aub.getFloat(j))
                {
                    min_u=this.aub.getFloat(j);
                }
                if(min_v>this.avb.getFloat(j))
                {
                    min_v=this.avb.getFloat(j);
                }
            }
            System.arraycopy(Bytes.toBytes(max_u),0,newb,16,4);
            System.arraycopy(Bytes.toBytes(max_v),0,newb,20,4);
            System.arraycopy(Bytes.toBytes(min_u),0,newb,24,4);
            System.arraycopy(Bytes.toBytes(min_v),0,newb,28,4);
            ProducerRecord<String,  byte[]> msg = new ProducerRecord<String,  byte[]>(this.topic,String.valueOf(i), newb);
            this.procuder.send(msg);
            System.out.println("(图幅序号:"+i/100+",切片序号:"+i%100+",起始点："+newb[0]+","+newb[1]+")");
       //     this.procuder.close(2, TimeUnit.SECONDS);

            hread.flag+=1;
        }

}
