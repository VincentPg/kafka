package cn.com.study.queue.kafkaimpl;

import java.util.Properties;

import cn.com.study.queue.QueueProduceFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
/**
 * 基于kafka的生产者队列
 * 
 * @author admin
 *
 */
public class KafkaQueueProduceFactory implements QueueProduceFactory {
	/**生产队列的配置**/
	private Properties produceProperties;

	public Properties getProduceProperties() {
		return produceProperties;
	}

	public void setProduceProperties(Properties produceProperties) {
		this.produceProperties = produceProperties;
	}
	/**
	 * 实例化生产队列。
	 * 见kafka生产demo
	 */
	public <T> KafkaQueueProducer<T> createQueueProducer() {
		ProducerConfig producerConfig = new ProducerConfig(produceProperties);
		Producer<String, String> produce = new Producer<String, String>(producerConfig);
		return new KafkaQueueProducer<T>(produce);
	}
}