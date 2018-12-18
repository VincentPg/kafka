package cn.com.study.queue;

import java.io.Closeable;
/**
 * 消费者队列
 * @author admin
 *
 * @param <T>
 */
public interface QueueConsumer<T> extends Closeable {
	/**
	 * 消费指定队列
	 * @param queueName
	 * @param clazz
	 */
	void pollData(String queueName, Class<T> clazz);
	/**
	 * 提交消费进度
	 */
	void commitOffset();
}
