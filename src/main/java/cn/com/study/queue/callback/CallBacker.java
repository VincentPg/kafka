package cn.com.study.queue.callback;

/**
 * @ClassName: Caller
 * @Description: 回调者类
 * @since JDK 1.8
 */
public class CallBacker<T> {
	private CallBack<T> method;

	public CallBacker(CallBack<T> method) {
		this.method = method;

	}

	public void call(T t) {
		if (method != null) {
			method.invokeMethod(t);
		}
	}
}