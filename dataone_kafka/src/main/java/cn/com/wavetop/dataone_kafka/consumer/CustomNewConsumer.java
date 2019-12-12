package cn.com.wavetop.dataone_kafka.consumer;

import java.util.*;

import cn.com.wavetop.dataone_kafka.client.ToBackClient;
import cn.com.wavetop.dataone_kafka.entity.ErrorLog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;

public class CustomNewConsumer {

	@Autowired
	private ToBackClient toBackClient;

	public void run() {

		Properties props = new Properties();
		// 定义kakfa 服务的地址，不需要将所有broker指定上 
		props.put("bootstrap.servers", "192.168.1.187:9092");
		// 制定consumer group 
		props.put("group.id", "sa_2123");
		// 是否自动确认offset 
		props.put("enable.auto.commit", "true");
		// 自动确认offset的时间间隔 
		props.put("auto.commit.interval.ms", "1000");
		// key的序列化类
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// value的序列化类 
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// 定义consumer 
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
		System.out.println(consumer.listTopics().keySet());
		Collection<List<PartitionInfo>> values = stringListMap.values();
		for (List<PartitionInfo> value : values) {
			for (PartitionInfo partitionInfo : value) {
				String topic = partitionInfo.topic();
				int jobId = Integer.valueOf(topic.split("_")[2]);
				String destlog = topic.split("_")[3];
				ErrorLog errorLog = new ErrorLog();
				//toBackClient.insertt(errorLog);
			}
		}


		// 消费者订阅的topic, 可同时订阅多个 
		//consumer.subscribe(Arrays.asList("first", "second","third"));
		consumer.subscribe(Arrays.asList("sink-error-logs"));
		while (true) {
			// 读取数据，读取超时时间为100ms 
			ConsumerRecords<String, String> records = consumer.poll(100);
			
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}
	}
}
