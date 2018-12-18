package cn.com.study.queue;

/**
 * 生产者队列工厂，用于常见生产者队列。
 * @author admin
 *
 */
public interface QueueProduceFactory {
	/**
	 * 生产者队列
	 * @return
	 */
	<T> QueueProducer<T> createQueueProducer();
}
