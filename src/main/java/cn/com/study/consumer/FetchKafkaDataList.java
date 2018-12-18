package cn.com.study.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.com.study.consumer.bean.KafkaDataBean;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class FetchKafkaDataList {
	// 最大获取数据条数，在fetchSize足够大时，为最少获取数量，在fetchSize不充足时，获取的数量为满足fetchSize最大的条数
	private long a_maxReads;

	// Kafka中topic
	private String a_topic;

	// topic中的分区数
	private int a_partition;

	// broker节点的ip或名称集合
	private List<String> a_seedBrokers;

	//
	private List<String> m_replicaBrokers = null;

	// Kafka consumer的超时时间
	private int timeOut;

	// Kafka consumer的缓冲区长度,默认0.1M
	private int consumerBufferSize;

	// Kafka 客户端名称
	private String clientName;

	// 每次从Kafka中抓取的数量，~Bytes
	private int fetchSize;

	// 数据在Kafka中的偏移量
	private long offset;

	private static int count = 0;

	public FetchKafkaDataList() {

		m_replicaBrokers = new ArrayList<String>();

	}

	/**
	 * @MethodName: FetchKafkaDataList
	 * @Description: 初始化consumer使用的参数
	 */

	public FetchKafkaDataList(Map<String, String> consumerConfig, Map<String, String> configMap) {

		this.a_topic = configMap.get("a_topic");
		this.a_partition = Integer.parseInt(configMap.get("a_partition"));
		this.a_maxReads = Long.parseLong(consumerConfig.get("a_maxReads"));
		m_replicaBrokers = new ArrayList<String>();

		if (consumerConfig.get("timeOut") == null || "".equals(consumerConfig.get("timeOut"))) {

			this.timeOut = 100000;

		} else {

			this.timeOut = Integer.parseInt(consumerConfig.get("timeOut"));
		}

		if (consumerConfig.get("consumerBufferSize") == null || "".equals(consumerConfig.get("consumerBufferSize"))) {

			this.consumerBufferSize = 100000;
		} else {

			this.consumerBufferSize = Integer.parseInt(consumerConfig.get("consumerBufferSize"));
		}

		this.clientName = "Client_" + a_topic + "_" + a_partition;

	}

	public FetchKafkaDataList setInitOffset(long offset) {
		this.offset = offset;
		return this;
	}

	public FetchKafkaDataList setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
		return this;
	}

	public FetchKafkaDataList setA_seedBrokers(List<String> a_seedBrokers) {
		this.a_seedBrokers = a_seedBrokers;
		return this;
	}

	/**
	 * @Description: 从kafka中 偏移量为offset的位置开始抓取数据总量为fetchSize的数据
	 * @return List<KafkaDataBean>
	 * @throws Exception
	 */

	public List<KafkaDataBean> fetchKafkaData() throws Exception {

		List<KafkaDataBean> kafkaDataList = new ArrayList<KafkaDataBean>();

		// 获取指定Topic partition的元数据
		PartitionMetadata metadata = findLeader(a_seedBrokers, a_topic, a_partition);
		if (metadata == null) {
			System.out.println("Can't find metadata for Topic and Partition. Exiting");
			return null;
		}
		if (metadata.leader() == null) {
			System.out.println("Can't find Leader for Topic and Partition. Exiting");
			return null;
		}

		String leadBroker = metadata.leader().host();
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), timeOut, consumerBufferSize,
				clientName);

		int numErrors = 0;
		long readOffset = 0;

		while (a_maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), timeOut, consumerBufferSize,
						clientName);
			}
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(a_topic, a_partition, offset, fetchSize).build();
			FetchResponse fetchResponse = consumer.fetch(req);

			// 获取数据出错处理
			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				short code = fetchResponse.errorCode(a_topic, a_partition);
				System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				if (numErrors > 5)
					break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for
					// the last element to reset
					// 处理offset非法的问题，用最新的offset
					readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(),
							clientName);
					System.out.println("LastOffset" + a_topic + a_partition + "::" + readOffset);
					continue;
				}
				consumer.close();
				consumer = null;
				// 更新leader broker
				leadBroker = findNewLeader(leadBroker, a_topic, a_partition);
				continue;
			}
			numErrors = 0;

			long numRead = 0;

			// 循环将数据封装到对象中，再放进list集合里
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
				KafkaDataBean kdb = new KafkaDataBean();
				long currentOffset = messageAndOffset.offset();
				// 当消息是压缩的时候，通过fetch获取到的是一个整块数据。块中解压后不一定第一个消息就是offset所指定的。就是说存在再次取到已读过的消息。
				if (currentOffset < readOffset) {
					System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset
							+ " Topic: " + a_topic + " Partition:" + a_partition);
					count++;

					if (count > 200) {

						throw new Exception("kafka数据读空次数为" + count);
					}
					continue;
				}
				count = 0;
				// 通过当前消息获取下一条消息的offset
				readOffset = messageAndOffset.nextOffset();

				ByteBuffer payload = messageAndOffset.message().payload();

				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);

				kdb.setTopic(a_topic);
				kdb.setPartition(a_partition);
				kdb.setOffset(messageAndOffset.offset());
				kdb.setMessage(new String(bytes, "UTF-8"));

				kafkaDataList.add(kdb);
				// System.out.println(kdb.getOffset() + ": " + new String(bytes, "UTF-8"));

				numRead++;
				a_maxReads--;
			}

			if (numRead == 0) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException ie) {
				}
			}

			if (kafkaDataList.size() == 0) {
				break;
			}
		}

		// System.out.println("------------------->>" + a_maxReads);
		if (consumer != null)
			consumer.close();
		// System.out.println("kafkaDataList------------------->>"+kafkaDataList);
		return kafkaDataList;

	}

	/**
	 * @Description 获取数据偏移量
	 * @param consumer
	 * @param topic
	 * @param partition
	 * @param whichTime
	 *            表示where to start reading data，两个取值
	 *            kafka.api.OffsetRequest.EarliestTime()，the beginning of the data
	 *            in the logs kafka.api.OffsetRequest.LatestTime()，will only stream
	 *            new messages
	 * @param clientName
	 * @return List<KafkaDataBean>
	 * 
	 */

	public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.out.println(
					"Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	/**
	 * @Description 重新调用findLeader获取leader broker 并且防止在切换过程中，取不到leader信息，加上sleep逻辑
	 * @param a_oldLeader
	 * @param a_topic
	 * @param a_partition
	 * @param a_port
	 * @return String
	 * @throws Exception
	 */
	private String findNewLeader(String a_oldLeader, String a_topic, int a_partition) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper a second to recover
				// second time, assume the broker did recover before failover,
				// or it was a non-Broker issue
				//
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		System.out.println("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	private PartitionMetadata findLeader(List<String> a_seedBrokers, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (String seedStr : a_seedBrokers) {
			String seed = seedStr.split(":")[0];
			int port = Integer.parseInt(seedStr.split(":")[1]);
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
						+ ", " + a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}
}
