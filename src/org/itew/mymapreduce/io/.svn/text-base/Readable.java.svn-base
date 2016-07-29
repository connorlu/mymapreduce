package org.itew.mymapreduce.io;

import java.io.IOException;

/**
 * 可读取的，一般为某个分片或某个map任务的中间结果
 * @author JiLu
 */
public interface Readable<K, V> {

	/**
	 * 这是一个游标，开始指向第一个可读资源之前，
	 * 返回下一个资源是否可读，
	 * 并使游标指向下一个资源
	 * @return 返回下一个资源是否可读，并使游标指向下一个资源
	 * @throws IOException 资源不能读或读时发生IO异常
	 */
	boolean next() throws IOException;

	/**
	 * 返回当前游标所指数据的键
	 * @return 返回当前游标所指数据的键
	 */
	K getKey();

	/**
	 * 返回当前游标所指数据的值
	 * @return 返回当前游标所指数据的值
	 */
	V getValue();
}
