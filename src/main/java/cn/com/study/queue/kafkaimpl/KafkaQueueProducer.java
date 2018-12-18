package cn.com.study.queue.kafkaimpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.com.study.queue.QueueProducer;
import cn.com.study.queue.entity.QueueMessage;
import cn.com.study.util.JsonUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * 基于kafka的生产队列
 * 
 * @author admin
 *
 * @param <T>
 */
public class KafkaQueueProducer<T> implements QueueProducer<T> {

	private Producer<String, String> producer;

	public Producer<String, String> getProducer() {
		return producer;
	}

	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	public KafkaQueueProducer(Producer<String, String> producer) {
		this.producer = producer;
	}
	public void saveMessage(QueueMessage<T> queueMessage) {
		try {
			KeyedMessage<String, String> message = new KeyedMessage<String, String>(queueMessage.getQueueName(),
					JsonUtil.objectToJson(queueMessage.getMessage()));
			producer.send(message);

		} catch (Exception e) {
			throw e;
		} finally {
			producer.close();
		}
	}
	public void saveMessages(List<QueueMessage<T>> queueMessages) {
		try {
			List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();

			queueMessages.forEach(message -> {
				KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(message.getQueueName(),
						JsonUtil.objectToJson(message.getMessage()));
				messages.add(keyedMessage);
			});

			producer.send(messages);
		} catch (Exception e) {
			throw e;
		} finally {
			producer.close();
		}
	}

	/**
	 * 生产者关闭
	 * 
	 * @see java.io.Closeable#close()
	 */
	public void close() throws IOException {
		producer.close();
	}
}