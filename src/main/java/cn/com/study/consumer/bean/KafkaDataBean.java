package cn.com.study.consumer.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import com.google.gson.Gson;

public class KafkaDataBean implements Serializable{
	
	private static final long serialVersionUID = 1L;

	private String topic;	//Kafka中的topic
	
	private int partition;	//Kafka中的分区号
	
	private Long offset;	//数据在kafka中的偏移量
		
	private String message;	//Kafka中的数据
	


	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	public String toString() {
		
		HashMap<String, Object> result = new HashMap<String, Object>();
		result.put("topic", topic);
		result.put("partition", partition);
		result.put("offset", offset);
		result.put("messge", message);
		
		return result.toString();
	}

	
}
