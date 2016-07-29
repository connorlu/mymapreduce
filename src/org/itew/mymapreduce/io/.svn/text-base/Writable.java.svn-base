package org.itew.mymapreduce.io;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * 可写的，按键值对进行写入
 * @author JiLu
 * @param K 写入的键类型
 * @param V 写入的值类型
 */
public interface Writable<K,V> extends Closeable{
	
	/**
	 * 写入方法
	 * @param key 写入的键
	 * @param value 写入的值
	 * @throws FileNotFoundException 
	 * @throws IOException 
	 */
	void write(K key,V value) throws IOException;
	
	/**
	 * 关闭文件的写入
	 */
	@Override
	void close();
	
}
