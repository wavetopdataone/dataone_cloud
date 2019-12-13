package cn.com.wavetop.dataone_kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

public class TestConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.156:9092");
        props.put("group.id", "test1");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //error-queue-logs
        TopicPartition topicPartition = new TopicPartition("task-41-EMPLOYEES", 0);
        consumer.assign(Arrays.asList(topicPartition));
        //consumer.subscribe(Arrays.asList(topic));
        consumer.seek(topicPartition,0);

        ConsumerRecords<String, String> records = null;
        while(true) {
            records = consumer.poll(100);
            if(!records.isEmpty()) {
                break;
            }
        }
        System.out.println(records.count());
        Iterator<ConsumerRecord<String, String>> iterable = records.iterator();
        int index = 0;
        while(index<1 && iterable.hasNext()) {
            //System.out.println("王成============================"+iterable.next().toString());
            String value = iterable.next().value();
            System.out.println("王成 =========================== " + value);
            index++;
        }

        /*ConsumerRecords<String, String> records = consumer.poll(10000);
        for (ConsumerRecord<String, String> record : records) {
            String value = record.value();
            System.out.println("value ================================= " + value);
        }*/
    }
}
