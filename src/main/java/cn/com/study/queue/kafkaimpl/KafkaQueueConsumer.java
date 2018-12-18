package cn.com.study.queue.kafkaimpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.com.study.queue.QueueConsumer;
import cn.com.study.queue.callback.CallBack;
import cn.com.study.queue.callback.CallBacker;
import cn.com.study.util.JsonUtil;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

/**
 * kafka消费者队列
 * 
 * @author admin
 *
 * @param <T>
 */
public class KafkaQueueConsumer<T> implements QueueConsumer<T> {

	private ConsumerConnector consumerConnector;
	private CallBack<T> callback;

	public KafkaQueueConsumer(ConsumerConnector consumerConnector, CallBack<T> callback) {
		this.consumerConnector = consumerConnector;
		this.callback = callback;
	}

	/**
	 * 消费方法
	 * 
	 * @see cn.jointwisdom.crawlersystem.common.queue.QueueConsumer#offData()
	 */

	public void pollData(String queueName, Class<T> clazz) {
		StringDecoder decoder = new StringDecoder(new VerifiableProperties());
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put(queueName, new Integer(1));
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector.createMessageStreams(map,
				decoder, decoder);
		KafkaStream<String, String> stream = consumerMap.get(queueName).get(0);

		ConsumerIterator<String, String> it = stream.iterator();

		/**
		 * xfni 修改与2017-03-23
		 * 跳过某条异常消息
		 */
		try{
			while (it.hasNext()) {
				long startTime = System.currentTimeMillis();
				String value = it.next().message();
				System.out.println("value:"+value);
				long endTime = System.currentTimeMillis();
				System.out.println(String.format("任务出队耗时： %s ms", endTime - startTime));
				CallBacker<T> caller = new CallBacker<T>(callback);
				T t = JsonUtil.jsonToObject(value, clazz);
				caller.call(t);
			}
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		
	}

	public void close() {
		if (consumerConnector != null) {
			consumerConnector.shutdown();
		}
	}

	public void commitOffset() {
		if (consumerConnector != null) {
			consumerConnector.commitOffsets(true);
		}
	}
}