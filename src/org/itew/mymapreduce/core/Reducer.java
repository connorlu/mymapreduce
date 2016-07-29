package org.itew.mymapreduce.core;

import java.io.IOException;

import org.itew.mymapreduce.io.Writable;


/**
 * 这是reduce任务要实现的接口
 * @author JiLu
 *
 * @param <IK> 输入键的类型
 * @param <IV> 输入值的类型
 * @param <OK> 输出键的类型
 * @param <OV> 输出值的类型
 */
public interface Reducer<IK,IV,OK,OV> {
	
	void reduce(IK key,Iterable<IV> values,Writable<OK,OV> writer) throws IOException;
	
}
