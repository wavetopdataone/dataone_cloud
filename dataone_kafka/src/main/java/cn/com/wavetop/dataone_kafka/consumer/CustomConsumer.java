package cn.com.wavetop.dataone_kafka.consumer;

import cn.com.wavetop.dataone_kafka.client.ToBackClient;
import cn.com.wavetop.dataone_kafka.config.SpringContextUtil;
import cn.com.wavetop.dataone_kafka.entity.ErrorLog;
import cn.com.wavetop.dataone_kafka.utils.FileUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class CustomConsumer extends Thread {

    @Override
    public void run() {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ToBackClient toBackClient = SpringContextUtil.getBean(ToBackClient.class);
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.156:9092");
        props.put("group.id", "test11");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();

        for (String s : stringListMap.keySet()) {
            if (s.split("-").length>2){

                //String info = s.split("-")[0];
                String info = s.split("-")[1];
                if (info.equals("error-logs")){

                    consumer.subscribe(Arrays.asList(s));
                    try{
                        while (true) {
                            ConsumerRecords<String, String> records =
                                    consumer.poll(10000);
                            for (ConsumerRecord<String, String> record : records) {
                                Long jobId = Long.valueOf(s.split("-")[2]);
                                String destTable= s.split("-")[3];
                                String value = record.value();
                                JSONObject jsonObject = JSONObject.parseObject(value);
                                String payload = (String)jsonObject.get("payload");
                                String[] split = payload.split(" ");
                                String errorinfo = split[1];
                                long timestamp = record.timestamp();
                                Date time = null;
                                try {
                                    time = simpleDateFormat.parse(simpleDateFormat.format(new Date(timestamp)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                //toBackClient.insertStatus(4);
                                String sourceTable = toBackClient.selectTable(jobId,destTable);
                                toBackClient.insertError(jobId,sourceTable,destTable,time,errorinfo);
                            }
                            consumer.commitAsync();//如果一切正常，我们使用commitAsync来提交，这样速度更快，而且即使这次提交失败，下次提交很可能会成功
                        }
                    }catch (Exception e){
                        throw new RuntimeException("commit failed");
                    }finally {
                        try {
                            consumer.commitSync();//关闭消费者前，使用commitSync，直到提交成成功或者发生无法恢复的错误
                        }finally {
                            consumer.close();
                        }
                    }
                }
            }
        }
    }
        /*File actionDir = new File("D:\\wangcheng\\dataone");
        // 调用打印目录方法
        printDir(actionDir);*/
        /*SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            // 创建字符输入缓冲流对象,并传递字符输入流对象，用来读取数据
            BufferedReader br = new BufferedReader(new FileReader("D:\\wangcheng\\dataone\\connect.log"));
            //ErrorLog errorLog = new ErrorLog();
            //注入ToBackClient
            ToBackClient toBackClient = SpringContextUtil.getBean(ToBackClient.class);
            Map<String, String> map = new HashMap<>();
            // 定义字符串，保存读取到的一行数据
            String line;
            while( ( line = br.readLine() ) != null ) {

                boolean error = line.contains("Error");
                if (error){
                    String[] split = line.split("\\{");
                    if (split.length > 1){
                        String string = split[1];
                        String[] split1 = string.split("}");
                        if (split1.length > 0) {
                            String s = split1[0];
                            String[] split2 = s.split(",");
                            if (split2.length > 0)

                                for (String s1 : split2) {

                                    String[] split3 = s1.split("=");

                                    map.put(split3[0].trim(),split3[1].trim());

                                }
                            String topic = map.get("topic");
                            String[] split3 = topic.split("-");
                            Long jobId = Long.valueOf(split3[2]);
                            String destname =split3[3].split("'")[0];
                            //String offset = map.get("offset");
                            String sourcename = toBackClient.selectTable(jobId,destname);
                            Long timestamp = Long.valueOf(map.get("timestamp"));
                            Date time = simpleDateFormat.parse(simpleDateFormat.format(new Date(timestamp)));
                            line = br.readLine();
                            toBackClient.insertt(jobId,sourcename,destname,time,line);
                        }
                    }
                }
            }
            // 关流
            br.close();
        }catch (Exception e){
            e.printStackTrace();
        }*/
        /*SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ToBackClient toBackClient = FrameSpringBeanUtil.getBean(ToBackClient.class);
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.156:9092");
        props.put("group.id", "test11");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();

        ErrorLog errorLog = new ErrorLog();*/



        /*for (String s : stringListMap.keySet()) {
            if (s.split("-").length>2){

                //String info = s.split("-")[0];
                String info = s.split("-")[1];
                if (info.equals("error")){
                    //int jobId = Integer.valueOf(s.split("-")[2]);
                    //String destlog = s.split("-")[3];
                    consumer.subscribe(Arrays.asList(s));

                    try{
                        while (true) {
                            ConsumerRecords<String, String> records =
                                    consumer.poll(10000);
                            for (ConsumerRecord<String, String> record : records) {
                                String value = record.value();
                                System.out.println("value ========================= " + value);
                                long timestamp = record.timestamp();
                                try {
                                    Date time = simpleDateFormat.parse(simpleDateFormat.format(new Date(timestamp)));
                                    errorLog.setOptTime(time);
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                errorLog.setContent(value);
                                System.out.println("time = " + errorLog.toString());
                                toBackClient.insertt(errorLog);
                            }
                            consumer.commitAsync();//如果一切正常，我们使用commitAsync来提交，这样速度更快，而且即使这次提交失败，下次提交很可能会成功
                        }
                    }catch (Exception e){
                        throw new RuntimeException("commit failed");
                    }finally {
                        try {
                            consumer.commitSync();//关闭消费者前，使用commitSync，直到提交成成功或者发生无法恢复的错误
                        }finally {
                            consumer.close();
                        }
                    }

                }
            }

        }*/



    public static void main(String[] args) throws Exception {
        /*File actionDir = new File("D:\\wangcheng\\dataone");

        // 调用打印目录方法
        printDir(actionDir);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");*/

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        /*ToBackClient toBackClient = SpringContextUtil.getBean(ToBackClient.class);*/
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.156:9092");
        props.put("group.id", "test1");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        HashMap<String, String> map = new HashMap<>();
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("error-queue-logs"));
        while(true){
            String topic=null;
            String partition;
            String offset;
            String errorinfo;
            ConsumerRecords<String, String> records =
                    consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                /*
                和错误日志里的信息是相同的
                {"schema":{"type":"string","optional":false},"payload":"\tat org.apache.kafka.connect.json.JsonConverter.toConnectData(JsonConverter.java:348)"}
                * */
                JSONObject jsonObject = JSONObject.parseObject(value);
                String payload = (String)jsonObject.get("payload");
                System.out.println("payload = " + payload);
                boolean error = payload.contains("ERROR Error");
                if (error){
                    String[] split = payload.split("\\{");
                    if (split.length > 1) {
                        String string = split[1];
                        String[] split1 = string.split("}");
                        if (split1.length > 1) {
                            String s = split1[0];
                            String[] split2 = s.split(",");
                            if (split2.length > 0) {
                                for (String s1 : split2) {
                                    String[] split3 = s1.split("=");
                                    map.put(split3[0].trim(), split3[1].trim());
                                }
                            }
                        }
                    }
                }
                topic = map.get("topic");
                partition = map.get("partition");
                offset = map.get("offset");
                if (payload.contains("Exception")){
                    String[] split = payload.split(":");
                    if (split.length>0){
                        errorinfo = split[1];
                        System.out.println("split = " + split);
                    }

                }
//                consumer.subscribe(Arrays.asList(topic));
            }


        }







        /*Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();

        for (String s : stringListMap.keySet()) {
            if (s.split("-").length>2){

                //String info = s.split("-")[0];
                String info = s.split("-")[1];
                if (info.equals("error-logs")){

                    consumer.subscribe(Arrays.asList(s));
                    try{
                        while (true) {
                            ConsumerRecords<String, String> records =
                                    consumer.poll(10000);
                            for (ConsumerRecord<String, String> record : records) {
                                Long jobId = Long.valueOf(s.split("-")[2]);
                                String destTable= s.split("-")[3];
                                String value = record.value();
                                JSONObject jsonObject = JSONObject.parseObject(value);
                                String payload = (String)jsonObject.get("payload");
                                String[] split = payload.split(" ");
                                String errorinfo = split[1];
                                long timestamp = record.timestamp();
                                Date time = null;
                                try {
                                    time = simpleDateFormat.parse(simpleDateFormat.format(new Date(timestamp)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                //toBackClient.insertStatus(jobId,destTable);
                                String sourceTable = toBackClient.selectTable(jobId,destTable);
                                toBackClient.insertError(jobId,sourceTable,destTable,time,errorinfo);
                            }
                            consumer.commitAsync();//如果一切正常，我们使用commitAsync来提交，这样速度更快，而且即使这次提交失败，下次提交很可能会成功
                        }
                    }catch (Exception e){
                        throw new RuntimeException("commit failed");
                    }finally {
                        try {
                            consumer.commitSync();//关闭消费者前，使用commitSync，直到提交成成功或者发生无法恢复的错误
                        }finally {
                            consumer.close();
                        }
                    }
                }
            }
        }*/



































        /*// 创建字符输入缓冲流对象,并传递字符输入流对象，用来读取数据
        try {
            // 创建字符输入缓冲流对象,并传递字符输入流对象，用来读取数据
            BufferedReader br = new BufferedReader(new FileReader("D:\\wangcheng\\dataone\\connect.log"));
            ErrorLog errorLog = new ErrorLog();
            //ToBackClient toBackClient = SpringContextUtil.getBean(ToBackClient.class);
            int index = 0;
            Map<String, String> map = new HashMap<>();
            // 定义字符串，保存读取到的一行数据
            String line;
            while( ( line = br.readLine() ) != null ) {

                boolean error = line.contains("ERROR");
                if (error){
                    String[] split = line.split("\\{");
                    if (split.length > 1){
                        String string = split[1];
                        String[] split1 = string.split("}");
                        if (split1.length > 1) {
                            String s = split1[0];
                            String[] split2 = s.split(",");
                            if (split2.length > 0)
                                for (String s1 : split2) {
                                    String[] split3 = s1.split("=");
                                    map.put(split3[0].trim(),split3[1].trim());
                                }
                            String topic = map.get("topic");
                            String[] split3 = topic.split("-");
                            Long jobId = Long.valueOf(split3[2]);
                            String destname = split3[3];
                            String offset = map.get("offset");
                            //String sourcename = toBackClient.selectTable(jobId);
                            Long timestamp = Long.valueOf(map.get("timestamp"));
                            Date time = simpleDateFormat.parse(simpleDateFormat.format(new Date(timestamp)));
                            errorLog.setJobId(jobId);
                            errorLog.setOptTime(time);
                            errorLog.setDestName(destname);
                            //errorLog.setSourceName(sourcename);
                            line = br.readLine();
                            errorLog.setContent(line);
                            //toBackClient.insertt(errorLog);
                        }
                    }
                }
            }
            // 关流
            br.close();
        }catch (Exception e){
            e.printStackTrace();
        }*/
    }

    //最好能定义一个指针能够实时定位下一个文件是否出现

    public static void printDir(File dir) {
        // 获取子文件和目录
        File[] files = dir.listFiles();

        File logfileName = null;
        //
        // 循环打印
        for (File file : files) {
            if (file.isFile()) {

                if (file.getName().equals("offset.txt")) {

                    String fileName = file.getName();

                    try {
                        // 创建字符输入缓冲流对象,并传递字符输入流对象，用来读取数据
                        BufferedReader br = new BufferedReader(new FileReader("D:\\wangcheng\\dataone\\"+fileName));
                        if (br.readLine() != null){
                            //则读取到文件名和哪一行的位移量

                        }else {
                            //找到第一个其他文件存进来

                        }

                        // 定义变量，保存有效字符个数
                        int len;
                        // 定义字符数组，作为装字符数据的容器
                        char[] cbuf = new char[2];
                        // 循环读取
                       /* while ((len = fr.read(cbuf))!=-1) {
                            System.out.println(new String(cbuf));
                        }
                        // 关闭资源
                        fr.close();*/
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } /*else {
                // 是目录，继续遍历,形成递归
                printDir(file);
            }*/
        }
    }
}