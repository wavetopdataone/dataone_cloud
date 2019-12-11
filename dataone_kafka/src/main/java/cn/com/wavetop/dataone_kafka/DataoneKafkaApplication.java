package cn.com.wavetop.dataone_kafka;

import cn.com.wavetop.dataone_kafka.client.ToBackClient;
import cn.com.wavetop.dataone_kafka.config.SpringContextUtil;
import cn.com.wavetop.dataone_kafka.consumer.CustomNewConsumer2;
import cn.com.wavetop.dataone_kafka.thread.version2.Action;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;

import org.springframework.cloud.client.SpringCloudApplication;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

//@SpringBootApplication
@SpringCloudApplication
@EnableFeignClients
public class DataoneKafkaApplication {


    @Autowired
    private static RestTemplate restTemplate;
    @Autowired
    private  ToBackClient toBackClient;


    public static void main(String[] args) throws Exception {

        ConfigurableApplicationContext context = SpringApplication.run(DataoneKafkaApplication.class, args);
        new SpringContextUtil().setApplicationContext(context);  //获取bean  为了注入kafkaTemplate
       new CustomNewConsumer2().start();


//        Action action = new Action();   // 主线程
////        action.start();  开启线程
////        直接让主线程跑
//        action.run();//当前main线程跑

//        new JobConsumerThread(80).start(); // 测试消费者
    }

    @Bean
    @LoadBalanced
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}
