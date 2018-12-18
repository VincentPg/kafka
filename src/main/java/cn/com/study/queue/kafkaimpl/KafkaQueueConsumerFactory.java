package cn.com.study.queue.kafkaimpl;

import java.util.Properties;

import cn.com.study.queue.QueueConsumer;
import cn.com.study.queue.QueueConsumerFactory;
import cn.com.study.queue.callback.CallBack;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * kafka消费者队列工厂
 * @author admin
 *
 */
public class KafkaQueueConsumerFactory implements QueueConsumerFactory {
	private Properties consumerProperties;

	public Properties getConsumerProperties() {
		return consumerProperties;
	}

	public void setConsumerProperties(Properties consumerProperties) {
		this.consumerProperties = consumerProperties;
	}

	public <T> QueueConsumer<T> createQueueConsumer(CallBack<T> callback) {
		ConsumerConfig consumerConfig = new ConsumerConfig(consumerProperties);
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

		return new KafkaQueueConsumer<T>(consumerConnector, callback);
	}
}