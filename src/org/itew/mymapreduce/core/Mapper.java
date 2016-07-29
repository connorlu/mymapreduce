package org.itew.mymapreduce.core;



/**
 * 这是map任务要实现的接口
 * @author JiLu
 *
 * @param <IK> 输入键的类型
 * @param <IV> 输入值的类型
 * @param <OK> 输出键的类型
 * @param <OV> 输出值的类型
 */
public interface Mapper<IK,IV,OK,OV> {
	
	/**
	 * map方法
	 * @param key 输入的键
	 * @param value 输入的值
	 * @param collection 将输出的键值对集合，将map方法的返回值put到out集合中
	 */
	void map(IK key,IV value,DataCollection<OK,OV> collection);
	
}
