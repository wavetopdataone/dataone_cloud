package cn.com.wavetop.dataone_kafka.consumer;

import java.text.SimpleDateFormat;
import java.util.*;

import cn.com.wavetop.dataone_kafka.client.ToBackClient;
import cn.com.wavetop.dataone_kafka.entity.ErrorLog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
	//获取另一个消息队列的信息
	public static String topicPartion(String topic,int partition,Long offset) {
		//SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		/*ToBackClient toBackClient = SpringContextUtil.getBean(ToBackClient.class);*/
		/*Properties props = new Properties();
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

		//Map<TopicPartition, Long> hashMap = new HashMap<>();
		TopicPartition topicPartition = new TopicPartition(topic, partition);

		consumer.assign(Arrays.asList(topicPartition));
		//consumer.subscribe(Arrays.asList(topic));
		consumer.seek(topicPartition,offset);
		String message=null;
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
			System.out.println("王成============================"+iterable.next().toString());
			message = iterable.next().value();
			index++;
		}
		return message;*/


		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.1.156:9092");
		props.put("group.id", "testm");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		//error-queue-logs
		TopicPartition topicPartition = new TopicPartition(topic, partition);
		consumer.assign(Arrays.asList(topicPartition));
		//consumer.subscribe(Arrays.asList(topic));
		consumer.seek(topicPartition,offset);

		ConsumerRecords<String, String> records = null;
		while(true) {
			records = consumer.poll(100);
			if(!records.isEmpty()) {
				break;
			}
		}
		//System.out.println(records.count());
		Iterator<ConsumerRecord<String, String>> iterable = records.iterator();
		int index = 0;
		String value = null;
		while(index<1 && iterable.hasNext()) {
			//System.out.println("王成============================"+iterable.next().toString());
			value = iterable.next().value();
			//System.out.println("王成 =========================== " + value);
			index++;
		}
		return value;
	}

}
