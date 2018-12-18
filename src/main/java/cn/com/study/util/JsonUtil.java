package cn.com.study.util;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

public class JsonUtil {
	/**
	 * 将对象反序列化成json字符串
	 * 
	 * @param obj
	 * @return
	 */
	public static String objectToJson(Object obj) {
		return JSON.toJSONString(obj);
	}

	public static String objectToJson(Object obj, String chartName) throws UnsupportedEncodingException {

		String str = JSON.toJSONString(obj);
		byte[] bytes = str.getBytes(chartName);
		String str2 = new String(bytes, chartName);
		return str2;
	}

	/**
	 * 将json字符串序列化成对象
	 * 
	 * @param jsonString
	 * @param objectClass
	 * @return
	 */
	public static <T> T jsonToObject(String jsonString, Class<T> objectClass) {
		return JSON.parseObject(jsonString, objectClass);
	}

	/**
	 * 将json字符串序列化成对象
	 * 
	 * @param jsonString
	 * @param objectClass
	 * @return
	 */
	public static <T> T jsonToObject(byte[] jsonbyte, Class<T> objectClass) {
		return jsonToObject(new String(jsonbyte), objectClass);
	}

	/**
	 * 将json字符串序列化成对象
	 * 
	 * @param jsonString
	 * @param objectClass
	 * @return
	 */
	public static <T> T jsonToObject(String jsonString, TypeReference<T> objectClass) {
		return JSON.parseObject(jsonString, objectClass);
	}

	/**
	 * 将json字符串序列化成对象
	 * 
	 * @param jsonString
	 * @param objectClass
	 * @return
	 */
	public static <T> T jsonToObject(byte[] jsonbyte, TypeReference<T> objectClass) {
		return jsonToObject(new String(jsonbyte), objectClass);
	}

	public static <T> List<T> jsonToObjectList(byte[] jsonbyte, Class<T> objectClass) {
		return jsonToObject(jsonbyte, new TypeReference<List<T>>() {
		});
	}

	public static <T> List<T> jsonToObjectList(String jsonString, Class<T> objectClass) {
		return jsonToObject(jsonString, new TypeReference<List<T>>() {
		});
	}

	public static <T> List<T> JsonToObjectArray(String jsonString, Class<T> objectClass) {
		return JSON.parseArray(jsonString, objectClass);
	}
}