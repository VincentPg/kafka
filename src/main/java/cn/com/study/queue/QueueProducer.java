package cn.com.study.queue;

import java.io.Closeable;
import java.util.List;

import cn.com.study.queue.entity.QueueMessage;

/**
 * 生产者队列
 * @author admin
 *
 * @param <T>
 */
public interface QueueProducer<T> extends Closeable {
	/**
	 * 生产单条信息
	 * @param queueMessage
	 */
	void saveMessage(QueueMessage<T> queueMessage);
	/**
	 * 批量生产信息
	 * @param queueMessages
	 */
	void saveMessages(List<QueueMessage<T>> queueMessages);
}
