package cn.com.study.queue;

import cn.com.study.queue.callback.CallBack;
/**
 * 消费队列工厂
 * @author admin
 *
 */
public interface QueueConsumerFactory {
	/**
	 * 创建消费队列
	 * @param callback
	 * @return
	 */
	<T> QueueConsumer<T> createQueueConsumer(CallBack<T> callback);
}
