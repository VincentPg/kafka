package cn.com.study.demo;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 参考：https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 * 
 * kafka生产者
 * 1、如果消费者和kafka不是一台机器，建议采用 hostname:port的方式去访问kafka，
 * 所以安装kafka的时候要注意其配置。
 * 2、注意Linux服务器中已经针对该端口进行防火墙开放。
 * 3、kafka关键类
 *   kafka.producer.ProducerConfig：给kafka应用程序提供配置信息。
 *   kafka.javaapi.producer.Producer：kafka生产者
 *   kafka.producer.KeyedMessage：kafka流记录
 *   
 * @author jdy-ww
 *
 */
public class ProducerDemo {
	public static void main(String[] args) {
		
//		long events = Long.parseLong(args[0]);
		long events = 100;
		
		Random rnd = new Random();
		
		Properties props = new Properties();
		
		props.put("zk.connect","kafka:2181");
		//kafka broker【必选】
		props.put("metadata.broker.list", "kafka:9092");
		//
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//
		props.put("partitioner.class", "cn.com.study.demo01.SimplePartitioner");
		//以异步方式将消息写入kafka,【非必选】
//		props.put("producer.type", "async");
		props.put("request.required.acks", "1");
		props.put("request.timeout.ms", "1000");
		 
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> producer = new Producer<String, String>(config);
	
		for (long nEvents = 0; nEvents < events; nEvents++) { 
			System.out.println("第 " + nEvents + "次写入信息");
            long runtime = new Date().getTime();  
            String ip = "192.168.2." + rnd.nextInt(255); 
            System.out.println("key:" + ip);
            String msg = runtime + ",www.example.com," + ip; 
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
            producer.send(data);
        }
		
        producer.close();
	}
}
