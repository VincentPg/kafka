package cn.com.study.queue.callback;

/**
 * @ClassName: CallInterface
 * @Description: 回调方法接口
 * @date 2015年7月6日 下午5:09:58
 * @since JDK 1.8
 */
public interface CallBack<T> {
	void invokeMethod(T t);
}
