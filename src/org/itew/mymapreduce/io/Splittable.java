package org.itew.mymapreduce.io;

import java.util.Collection;

/**
 * 可分割的输入资源
 * @author JiLu
 *
 */
public interface Splittable<K,V> {
	
	/**
	 * 返回分片的个数
	 * @return 返回分片的个数
	 */
	int getFragmentCount();
	
	/**
	 * 返回各个分片的读者
	 * @return 返回各个分片的读者
	 */
	Collection<? extends Readable<K,V>> getReaders();
	
	
}
