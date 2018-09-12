package read;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import ucar.ma2.*;
import ucar.nc2.*;
import ucar.nc2.dataset.NetcdfDataset;
import java.util.Properties;
import java.io.BufferedWriter;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import java.util.Scanner;
import java.util.*;


public class hread {

    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;
    public static byte[] value;
    static  int flag=0;

    public static void main(String[] args) throws IOException, InvalidRangeException, InterruptedException {

        NetcdfFile ncfile_source = null;
        NetcdfFile ncfile_source2 = null;
        String location_destination = "D:/tmp/testWrite1.nc";
        NetcdfFileWriter creater=null;
        NetcdfFileWriter editer=null;
        int width=2880;
        int height=560;

        ///////////////////////////////////////////////////
        // 读uwnd
       /*  String value_index[]=new String[24];
        String value_index2[]=new String[24];
       for (int index=0;index<1;index++)
        {
            value_index[index]= "u10m"+String.format("%03d", index);
            value_index2[index]= "v10m"+String.format("%03d", index);
        }*/
        String value_index[]= {"lon","lat","level","time","flag","stat","tstr","ystr","u10m000"};
        String value_index2[]= {"lon","lat","level","time","flag","stat","tstr","ystr","v10m000"};
        ncfile_source = NetcdfDataset.open(args[0]);
        Variable[] value_source = new Variable[24];
        for(int i=0;i<value_index.length;i++)

            value_source[i] = ncfile_source.findVariable(value_index[i]);

        ncfile_source2 = NetcdfDataset.open(args[1]);
        Variable[] value_source2 = new Variable[24];
        for (int i = 0; i < value_index2.length; i++)
            value_source2[i] = ncfile_source2.findVariable(value_index2[i]);
        System.out.println(ncfile_source.getVariables());
        float[] float_properties=new float[8];
        float[] lat_source = (float[]) ncfile_source.findVariable("lat").read("0:"+(height-1)).copyToNDJavaArray();
        float[] lon_source = (float[]) ncfile_source.findVariable("lon").read("0:"+(width-1)).copyToNDJavaArray();
        float_properties[0]=lat_source[0];//纬度起始
        float_properties[1]=lat_source[lat_source.length-1];//纬度终止
        float_properties[2]=lon_source[0];//经度起始
        float_properties[3]=lon_source[lon_source.length-1];//经度终止
        float_properties[4]=0;//umax
        float_properties[5]=0;//vmax
        float_properties[6]=0;//umin
        float_properties[7]=0;//vmin
        int[] int_properties=new int[2];
        int_properties[0]=width;//位图宽度
        int_properties[1]=height;//位图高度
        // float[] lon_source = (float[]) value_source[0].read("0:592").copyToNDJavaArray();
        //     float[] lat_source = (float[]) value_source[1].read("0:392").copyToNDJavaArray();
        float[] level_source = (float[]) value_source[2].read("0:0").copyToNDJavaArray();
        float[] time_source = (float[]) value_source[3].read("54:61").copyToNDJavaArray();
        float[] flag_source = (float[]) value_source[4].read("0:0").copyToNDJavaArray();
        float[] stat_source = (float[]) value_source[5].read("0:0").copyToNDJavaArray();
        float[] tstr_source = (float[]) value_source[6].read("0:0").copyToNDJavaArray();
        float[] ystr_source = (float[]) value_source[7].read("0:0").copyToNDJavaArray();

        //////////////
        //取数据
        int[][] origin = new int[][] {{54, 0,0, 0},{55, 0,0, 0},{56, 0,0, 0},{57, 0,0, 0},{58, 0,0, 0},{59, 0,0, 0},{60, 0,0, 0},{61, 0,0, 0}};
        int[] size = new int[] {1,1,height,width};
        Array[] data_source =new Array[8];
        for(int i=0;i<origin.length;i++)
            data_source[i] = value_source[8].read(origin[i],size);
        //vwnd读取
        // ncfile_source2 = NetcdfDataset.open(args[1]);
        //System.out.println(ncfile_source2.getVariables());

        for(int i=0;i<value_index2.length;i++)
            value_source2[i] = ncfile_source2.findVariable(value_index2[i]);
        //Array data_source2 = value_source2[8].read(origin,size);
        Array[] data_source2 =new Array[8];
        for(int i=0;i<origin.length;i++)
            data_source2[i] = value_source2[8].read(origin[i],size);

        //线程池与producer配置
     /*   ExecutorService ex= Executors.newFixedThreadPool(4);
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.200:9092,192.168.1.201:9092,192.168.1.202:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> procuder = new KafkaProducer<String, byte[]>(props);
        String topic = "photo";
        int time= 62;
        int level=1;
        int pre_len=1;
        //打碎图幅发送
        for(int xtime=58;xtime<time;xtime++) {
            for(int xlevel=0;xlevel<level;xlevel++) {
                int[][] origin = new int[][]{{xtime, xlevel, 0, 0}, {xtime, xlevel, 0, width/2}, {xtime, xlevel, height/2, 0}, {xtime, xlevel, height/2, width/2}};
                int[] size = new int[]{1, 1, height/2, width/2};
                for(int prediction=0;prediction<pre_len;prediction++) {
                    Array[] data_source = new Array[4];
                    Array[] data_source2 = new Array[4];
                    //uwnd读取
                    for (int i = 0; i < origin.length; i++)
                        data_source[i] = value_source[prediction].read(origin[i], size);
                    //vwnd读取
                    for (int i = 0; i < origin.length; i++)
                        data_source2[i] = value_source2[prediction].read(origin[i], size);
                //    Array datasource3=value_source[prediction].read(new int[]{xtime,xlevel,0,0},new int[]{1,1,height,width});
                  //  Array datasource4=value_source2[prediction].read(new int[]{xtime,xlevel,0,0},new int[]{1,1,height,width});
                    //文件存储
                //    storage(datasource3, datasource4,float_properties,int_properties);
                  //  执行线程
                    for (int i = 0; i < origin.length; i++) {
                        ex.execute(new sender(data_source[i], data_source2[i], (xlevel+1) * 1000000000000L + xtime * 1000000L +prediction*1000+ i, topic, procuder,float_properties,int_properties));
                    }
                    Thread.sleep(500);
                }
            }
            Thread.sleep(600000);
        }

        //列出topic的相关信息
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>() ;
        partitions = procuder.partitionsFor(topic);
        for(PartitionInfo p:partitions)
        {
            System.out.println(p);
        }
        System.out.println("send message over.");
       while(flag!=4*time)
       {
          //  System.out.print(flag);
       }
           procuder.close(2, TimeUnit.SECONDS);
        ex.shutdown();*/


        ////////////////////////////////
        //写入hbase
       /* byte[][] add = new byte[(int) data_source.getSize()*2][];
        for (int i = 0; i < data_source.getSize(); i++) {
            add[i*2] = Bytes.toBytes(data_source.getFloat(i));
            add[i*2+1] = Bytes.toBytes(data_source2.getFloat(i));
        }
        byte[] newb=Bytes.add(add);
        init();
        Put put=new Put(Bytes.toBytes("row4"));
       // String aa=String.valueOf(data_source);
        put.addColumn(Bytes.toBytes("group1"),Bytes.toBytes("val"),newb);
        HTable table = (HTable) connection.getTable(TableName.valueOf("table1"));
        table.put(put);*/

        ///////////////////
        //写入到js
        for(int i=0;i<data_source.length;i++) {
            String sDestFile = "D:/tmp/vector_field_data"+i+".js";
            File destFile = new File(sDestFile);
            if (!destFile.exists()) {
                destFile.createNewFile();
            }
            FileWriter fw = new FileWriter(destFile.getAbsoluteFile());
            BufferedWriter bufferWritter = new BufferedWriter(fw);
            bufferWritter.write("var windData = {\r\n");
            bufferWritter.write("timestamp:\"" + time_source[i]+"\",\r\n ");
            bufferWritter.write("level_stamp:\"" + level_source[0]+"\",\r\n ");
            bufferWritter.write("x0:" + lon_source[0] + ",\r\n ");
            bufferWritter.write("y0:" + lat_source[0] + ",\r\n ");
            bufferWritter.write("x1:" + lon_source[lon_source.length - 1] + ",\r\n ");
            bufferWritter.write("y1:" + lat_source[lat_source.length - 1] + ",\r\n ");
            bufferWritter.write("gridWidth:" + width + ",\r\n ");
            bufferWritter.write("gridH  eight:" + height + ",\r\n ");
            bufferWritter.write("field : [" + "\r\n ");
            for (int j = 0; j < data_source[i].getSize() - 1; j++) {
                bufferWritter.write(String.valueOf(data_source[i].getFloat(j)) + ",");
                bufferWritter.write(String.valueOf(data_source2[i].getFloat(j)) + ",");
                bufferWritter.newLine();
            }

            bufferWritter.write(String.valueOf(data_source[i].getFloat((int) (data_source[i].getSize() - 1))) + ",");
            bufferWritter.write(String.valueOf(data_source2[i].getFloat((int) (data_source2[i].getSize() - 1))) + "\r\n]");
            bufferWritter.newLine();
            bufferWritter.write("}");
            bufferWritter.close();
        }



        ///////////////////////////////
        // 写到nc文件
      /*  File file = new File(location_destination);

        if(!file.exists()) {
            //创建文件
            creater = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, location_destination, null);
            Dimension latDim = creater.addDimension(null, "lat", height);
            Dimension lonDim = creater.addDimension(null, "lon",width);
            Dimension timeDim = creater.addDimension(null, "time", 1);
            Dimension levelDim = creater.addDimension(null, "level", 1);

            Variable lon_d = creater.addVariable(null, "lon", DataType.FLOAT, "lon");
            lon_d.addAttribute(new Attribute("units", "degrees_east"));
            Variable lat_d = creater.addVariable(null, "lat", DataType.FLOAT, "lat");
            lat_d.addAttribute(new Attribute("units", "degrees_north"));
            Variable level_d = creater.addVariable(null, "level", DataType.FLOAT, "level");
            level_d.addAttribute(new Attribute("units", "millibars"));
            Variable time_d = creater.addVariable(null, "time", DataType.FLOAT, "time");
            time_d.addAttribute(new Attribute("units", "days since 2017-11-01 00:00:00"));


            creater.addVariable(null, "flag", DataType.CHAR, "time");
            creater.addVariable(null, "stat", DataType.CHAR, "time");
            creater.addVariable(null, "tstr", DataType.DOUBLE, "time");
            creater.addVariable(null, "ystr", DataType.DOUBLE, "time");

            Variable uwnd = creater.addVariable(null, "uwnd000", DataType.FLOAT, "time level lat lon");
            // uwnd.addAttribute(new Attribute("units", "days since 2017-11-01 00:00:00"));
            creater.create();
        }
        if (null != creater) {
            creater.close();
        }

        //写入文件
        editer = NetcdfFileWriter.openExisting(location_destination);
        Variable lon_e = editer.findVariable("lon");
        editer.write(lon_e,Array.factory(lon_source));
        Variable lat_e = editer.findVariable("lat");
        editer.write(lat_e,Array.factory(lat_source));
        Variable level_e = editer.findVariable("level");
        editer.write(level_e,Array.factory(level_source));
        Variable time_e = editer.findVariable("time");
        editer.write(time_e,Array.factory(time_source));
        Variable flag_e = editer.findVariable("flag");
        editer.write(flag_e,Array.factory(flag_source));
        Variable stat_e = editer.findVariable("stat");
        editer.write(stat_e,Array.factory(stat_source));
        Variable tstr_e = editer.findVariable("tstr");
        editer.write(tstr_e,Array.factory(tstr_source));
        Variable ystr_e = editer.findVariable("ystr");
        editer.write(ystr_e,Array.factory(ystr_source));
        Variable uwnd_e = editer.findVariable("uwnd000");
        ArrayFloat.D4 uwnd_data = new ArrayFloat.D4(1, 1, height,width);
        Index ima = uwnd_data.getIndex();

        for (int latIdx = 0; latIdx < height; latIdx++) {
            for (int lonIdx = 0; lonIdx < width; lonIdx++) {
                uwnd_data.setFloat(ima.set(0,0, latIdx, lonIdx), data_source.getFloat(latIdx*width+lonIdx));
            }
        }
        editer.write(uwnd_e,uwnd_data);*/

        if (null != ncfile_source)
        {
            ncfile_source.close();
        }

        if (null != editer) {
            editer.close();
        }
        //test(location_destination);
    }
    public  static  Array translate(Array array)
    {
        Array result=new ArrayFloat.D1(1440*281);
        for(int j=0;j<1440;j++)
            for(int i=0;i<281;i++)
            {
                ((ArrayFloat.D1) result).set(i*1440+j,array.getFloat(i+j*281));
            }
        return result;
    }
    public  static void  storage (Array array1,Array array2,float[] floats,int[] ints) throws IOException {
        String sDestFile = "D:/tmp/array"+".txt";
        File destFile = new File(sDestFile);
        if (!destFile.exists()) {
            destFile.createNewFile();
        }
        byte[] result=new byte[(int)array1.getSize()*2+40];
        float max_u=0;
        float max_v=0;
        float min_u=0;
        float min_v=0;
        for(int i=0;i<array1.getSize();i++)
        {
            result[i*2+40]=(byte) ((int)(array1.getFloat(i)));
            result[i*2+41]=(byte) ((int)(array2.getFloat(i)));
            if(max_u<array1.getFloat(i))
            {
                max_u=array1.getFloat(i);
            }
            if(max_v<array2.getFloat(i))
            {
                max_v=array2.getFloat(i);
            }
            if(min_u>array1.getFloat(i))
            {
                min_u=array1.getFloat(i);
            }
            if(min_v>array2.getFloat(i))
            {
                min_v=array2.getFloat(i);
            }
        }
        floats[4]=max_u;
        floats[5]=max_v;
        floats[6]=min_u;
        floats[7]=min_v;
        for(int i=0;i<floats.length;i++)
        {
            System.arraycopy(Bytes.toBytes(floats[i]),0,result,i*4,4);
        }
        for(int i=0;i<ints.length;i++)
        {
            System.arraycopy(Bytes.toBytes(ints[i]),0,result,i*4+32,4);
        }
        FileWriter fw = new FileWriter(destFile.getAbsoluteFile());
        BufferedWriter bufferWritter = new BufferedWriter(fw);
        bufferWritter.write("[");

        for (int j = 0; j < result.length - 1; j++) {
            bufferWritter.write(Integer.valueOf(result[j]) + ",");
            if(j%5760==40)
            {
                bufferWritter.newLine();
            }

        }

        bufferWritter.write(String.valueOf(Integer.valueOf(result[result.length-1])));

        bufferWritter.newLine();
        bufferWritter.write("]");
        bufferWritter.close();
    }
    public static void test(String location) throws IOException, InvalidRangeException {
        NetcdfFile ncfile_source = null;

        int width=100;
        int height=100;

        ///////////////////////////////////////////////////
        // 读
        ncfile_source = NetcdfDataset.open(location);
        //System.out.println(ncfile.getVariables());
        String value_index[]= {"lon","lat","level","time","flag","stat","tstr","ystr","uwnd000"};

        Variable[] value_source = new Variable[9];
        for(int i=0;i<value_index.length;i++)
            value_source[i] = ncfile_source.findVariable(value_index[i]);

        float[] lon_source = (float[]) value_source[0].read("0:99").copyToNDJavaArray();
        float[] lat_source = (float[]) value_source[1].read("0:99").copyToNDJavaArray();
        float[] level_source = (float[]) value_source[2].read("0:0").copyToNDJavaArray();
        Array time_source =  value_source[3].read("0:0");
        Array flag_source =  value_source[4].read("0:0");
        Array stat_source = value_source[5].read("0:0");
        Array tstr_source = value_source[6].read("0:0");
        Array ystr_source =  value_source[7].read("0:0");
        int[] origin = new int[] {0, 0,0, 0};
        int[] size = new int[] {1,1,height,width};
        Array data_source = value_source[8].read(origin,size);

        System.out.println(data_source.getSize());
    }
    public static void init() throws IOException {
        conf = HBaseConfiguration.create();
        //conf.set("hbase.master", "192.168.1.200:16000");
        //conf.set("hbase.rootdir", "hdfs://192.168.1.200:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "192.168.1.200");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf("table1"));
        table.addFamily(new HColumnDescriptor("group1")); //创建表时至少加入一个列组

        if(!admin.tableExists(table.getTableName())){
            //admin.disableTable(table.getTableName());
            //admin.deleteTable(table.getTableName());
            admin.createTable(table);
        }

    }
}
