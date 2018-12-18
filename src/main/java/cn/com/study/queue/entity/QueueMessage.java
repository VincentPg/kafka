package cn.com.study.queue.entity;
/**
 * 消息队列：队列名+消息
 * @author admin
 *
 * @param <T>
 */
public class QueueMessage<T> {
	/**队列名**/
	private String queueName;
	/**消息名**/
	private T message;

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public T getMessage() {
		return message;
	}

	public void setMessage(T message) {
		this.message = message;
	}
}