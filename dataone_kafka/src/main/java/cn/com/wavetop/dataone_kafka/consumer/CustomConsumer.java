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
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class CustomConsumer extends Thread {
    /**
     * 消费topic中的信息,将错误信息插入到错误错误日志表中
     *
     */
    @Override
    public void run() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ToBackClient toBackClient = SpringContextUtil.getBean(ToBackClient.class);
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.156:9092");
        props.put("group.id", "test12");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        HashMap<String, String> map = new HashMap<>();
        HashMap<String, String> iteratorList = new HashMap<>();
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅错误队列信息
        consumer.subscribe(Arrays.asList("error-queue-logs"));
        try{
        while(true) {
            //String topic=null;
            //int partition= 0;
            //Long offset= -1L;
            //String message=null;
            ConsumerRecords<String, String> records = null;
            while (true) {
                records = consumer.poll(100);
                if (!records.isEmpty()) {
                    break;
                }
            }
            //获取record迭代器
            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();

            while (iterator.hasNext()) {
                String value = iterator.next().value();
                JSONObject jsonObject = JSONObject.parseObject(value);
                String payload = (String) jsonObject.get("payload");

                boolean error = payload.contains("ERROR Error");

                if (error) {

                    String topic = null;
                    int partition = 0;
                    Long offset = 0L;
                    String message = null;
                    //第一次分割的片段
                    String[] split = payload.split("\\{");
                    if (split.length > 1) {
                        String string = split[1];
                        //第二次分割的片段
                        String[] split1 = string.split("}");
                        if (split1.length > 1) {
                            String s = split1[0];
                            //第三次分割的片段
                            String[] split2 = s.split(",");
                            if (split2.length > 0) {
                                for (String s1 : split2) {
                                    //第四次分割的片段
                                    String[] split3 = s1.split("=");
                                    map.put(split3[0].trim(), split3[1].trim());
                                }
                            }
                        }
                    }

                    if (map.get("topic") != null) {
                        topic = map.get("topic").replace("'", "");
                    }
                    Long jobId = 0L;
                    String destTable = null;
                    //从topic的名字中截取jobId,destTable
                    if (topic != null) {
                        String[] split1 = topic.split("-");
                        jobId = Long.valueOf(split1[1]);
                        destTable = split1[2];
                    }
                    if (map.get("partition") != null) {
                        partition = Integer.valueOf(map.get("partition"));
                    }
                    if (map.get("offset") != null) {
                        offset = Long.valueOf(map.get("offset"));
                    }

                    String time = null;
                    if (map.get("timestamp") != null) {
                        Long timestamp = Long.valueOf(map.get("timestamp"));
                        try {
                            time = simpleDateFormat.format(new Date(timestamp));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    int index = 0;
                    if (index<1 && iterator.hasNext()) {
                        String value1 = iterator.next().value();
                        JSONObject jsonObject1 = JSONObject.parseObject(value1);
                        String payload1 = (String) jsonObject1.get("payload");
                        boolean exception = payload1.contains("Exception");
                        if (exception) {
                            String[] split3 = payload1.split(":");
                            if (split3.length > 0) {
                                //errorflag是错误标志,如果是普通异常就往中台传1,如果死进程则传2
                                if (split3[0].contains("Exception")) {
                                    Integer errorflag = 1;
                                    String errortype = split3[0];
                                    String sourceTable = toBackClient.selectTable(jobId, destTable, time,errorflag);
                                    message = CustomNewConsumer.topicPartion(topic, partition, offset);

                                    System.out.println("sourceTable = " + sourceTable);
                                    //远程调用插入错误日志信息
                                    toBackClient.insertError(jobId, sourceTable, destTable, time, errortype, message);

                                }
                            }
                        }
                        index ++;
                    }
                }

                /**
                 * 如果包含ERROR而不包含ERROR Error的要放入系统日志表中
                 * 如果是死进程异常,则将异常插入到syslog表中,便修改状态
                 * 如果是普通的系统异常,则直接插入到syslog表中,不需要修改状态
                 */

                if (!payload.contains("ERROR Error")&&payload.contains("ERROR")) {
                    if (payload.contains("being killed")){
                        HashMap<String, String> hashMap = new HashMap<>();
                        String[] split = payload.split("\\{");
                        if (split.length > 1) {
                            String string = split[1];
                            //第二次分割的片段
                            String[] split1 = string.split("}");
                            if (split1.length > 0) {
                                String s = split1[0];
                                //第三次分割的片段
                                String[] split2 = s.split("=");
                                hashMap.put(split2[0].trim(),split2[1].trim());
                            }
                        }
                        String topic = null;
                        if (hashMap.get("id") != null) {
                            topic = hashMap.get("id");

                            String[] split1 = topic.split("-");

                            Long jobId = null;
                            String destTable = null;
                            if (split1.length > 3) {

                                jobId = Long.valueOf(split1[2]);
                                destTable = split1[3];
                            }
                            String time = "2019-12-16 10:23:32";
                            Integer errorflag = 2;
                            String sourceTable = toBackClient.selectTable(jobId, destTable, time, errorflag);
                        }
                    }
                    if (iterator.hasNext()){
                        String exception = iterator.next().value();
                        JSONObject jsonObject1 = JSONObject.parseObject(exception);
                        String payload1 = (String) jsonObject1.get("payload");
                        String method = "com.wavetop.dataone_kafka.consumer.CustomConsumer";
                        if (payload1.contains("Exception")) {
                            String[] split = payload1.split(":");
                            for (String syserror : split) {
                                if (syserror.contains("Exception")){
                                    toBackClient.inserSyslog(syserror,method);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            consumer.commitAsync();//如果一切正常，使用commitAsync来提交，这样速度更快，而且即使这次提交失败，下次提交很可能会成功
        }
    }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("commit failed");
    }finally {
            try {
            consumer.commitSync();//关闭消费者前，使用commitSync，直到提交成成功或者发生无法恢复的错误
        }finally {
            consumer.close();
        }
    }










       /* SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ToBackClient toBackClient = SpringContextUtil.getBean(ToBackClient.class);
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.156:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        HashMap<String, String> map = new HashMap<>();
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅错误队列信息
        consumer.subscribe(Arrays.asList("error-queue-logs"));
//        try{
            while(true){
                String topic=null;
                int partition= 0;
                Long offset= -1L;
                String message=null;
                ConsumerRecords<String, String> records =
                        consumer.poll(10000);
                for (ConsumerRecord<String, String> record : records) {
                    String errortype=null;
                    String value = record.value();

                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String payload = (String)jsonObject.get("payload");
                    //System.out.println("payload = " + payload);
                    boolean error = payload.contains("ERROR Error");
                    boolean error1 = payload.contains("ERROR");
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
                    if (error==false && error1==true){
                        topic = null;
                    }
                    if (map.get("topic")!=null){

                        topic = map.get("topic").replace("'","");
                    }
                    Long jobId = 0L;
                    String destTable = null;
                    if (topic != null){
                        String[] split = topic.split("-");
                        jobId = Long.valueOf(split[1]);
                        destTable = split[2];
                    }
                    if (map.get("partition") != null){
                        partition = Integer.valueOf(map.get("partition"));
                    }
                    if (map.get("offset") != null){
                        offset = Long.valueOf(map.get("offset"));
                    }
                    Date time = null;
                    if (map.get("timestamp")!=null){
                        Long timestamp = Long.valueOf(map.get("timestamp"));
                        try {
                            time = simpleDateFormat.parse(simpleDateFormat.format(new Date(timestamp)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    //Caused by: org.apache.kafka.common.errors.InvalidTopicException:
                    //
                    if (payload.contains("Exception")){
                        String[] split = payload.split(":");
                        if (split.length>0){
                            if (split[0].contains("Exception")&& !split[0].contains("ConnectException")){
                                errortype = split[0];
                            }
                            //System.out.println("split = " + split);
                        }
                    }
                    //System.out.println("topic = " + topic);
                    //这里是两行为一个周期,当topic和errorinfo不为空时,将所有的信息发送到数据库
                    if (null != topic && null!=errortype){

                        //远程调用查询源端表名
                        String sourceTable = toBackClient.selectTable(jobId, destTable,time);
                        message = CustomNewConsumer.topicPartion(topic, partition, offset);
                        System.out.println("sourceTable = " + sourceTable);
                        topic = null;
                        //远程调用插入错误日志信息
                        toBackClient.insertError(jobId,sourceTable,destTable,time,errortype,message);
                    }
                }
            }*/
        /*}catch (Exception e){
            throw new RuntimeException("commit failed");
        }finally {
            try {
                consumer.commitSync();//关闭消费者前，使用commitSync，直到提交成成功或者发生无法恢复的错误
            }finally {
                consumer.close();
            }
        }*/










        /*SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
        }*/
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
        //kafka-connect.log
        //BufferedReader br = new BufferedReader(new FileReader("D:\\wangcheng\\dataone\\kafka-connect.log"));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ToBackClient toBackClient = SpringContextUtil.getBean(ToBackClient.class);
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.156:9092");
        props.put("group.id", "testf");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        HashMap<String, String> map = new HashMap<>();
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅错误队列信息
        consumer.subscribe(Arrays.asList("error-queue-logs"));
        //try{
        while(true){
            //String topic=null;
            //int partition= 0;
            //Long offset= -1L;
            //String message=null;

            ConsumerRecords<String, String> records = null;
            while(true) {
                records = consumer.poll(100);
                if(!records.isEmpty()) {
                    break;
                }
            }
            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();

            while(iterator.hasNext()){
                String value = iterator.next().value();
                JSONObject jsonObject = JSONObject.parseObject(value);
                String payload = (String)jsonObject.get("payload");
                //System.out.println("payload = " + payload);
                boolean error = payload.contains("ERROR Error");
                //boolean error1 = payload.contains("ERROR");
                if (error){

                    String topic = null;
                    int partition= 0;
                    Long offset= 0L;
                    String message=null;
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
                    if (map.get("topic")!=null){
                        topic = map.get("topic").replace("'","");
                    }
                    Long jobId = 0L;
                    String destTable = null;
                    if (topic != null){
                        String[] split1 = topic.split("-");
                        jobId = Long.valueOf(split[1]);
                        destTable = split[2];
                    }
                    if (map.get("partition") != null){
                        partition = Integer.valueOf(map.get("partition"));
                    }
                    if (map.get("offset") != null){
                        offset = Long.valueOf(map.get("offset"));
                    }
                    Date time = null;
                    if (map.get("timestamp")!=null){
                        Long timestamp = Long.valueOf(map.get("timestamp"));
                        try {
                            time = simpleDateFormat.parse(simpleDateFormat.format(new Date(timestamp)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    if (iterator.hasNext()){
                        String value1 = iterator.next().value();
                        JSONObject jsonObject1 = JSONObject.parseObject(value1);
                        String payload1 = (String)jsonObject1.get("payload");
                        boolean exception = payload1.contains("Exception");
                        if (exception){
                            String[] split3 = payload1.split(":");
                            if (split3.length>0){
                                if (split3[0].contains("Exception")&& !split[0].contains("ConnectException")&& !split[0].contains("RetriableException")){
                                    Integer errorflag = 1;
                                    String errortype = split[0];
                                    //String sourceTable = toBackClient.selectTable(jobId, destTable,time,errorflag);
                                    message = CustomNewConsumer.topicPartion(topic, partition, offset);
                                    //System.out.println("sourceTable = " + sourceTable);
                                    //远程调用插入错误日志信息
                                    //toBackClient.insertError(jobId,sourceTable,destTable,time,errortype,message);
                                }
                            }
                        }
                    }
                }
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
    }
}