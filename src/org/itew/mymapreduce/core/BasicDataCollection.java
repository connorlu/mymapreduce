package org.itew.mymapreduce.core;

import java.util.List;
import java.util.Map;

public abstract class BasicDataCollection<K,V> implements FrameworkDataCollection<K, V> {

	protected final Map<K,List<V>>[] datas;
	protected final Partition<K> partition;
	protected final int reducerCount;
	
	@SuppressWarnings("unchecked")
	public BasicDataCollection(Configuration configuration,Partition<K> partition){
		this.partition = partition;
		this.reducerCount = configuration.getMaxReducerCount();
		this.datas = new Map[reducerCount];
		
		initMaps();
		
	}
	
	protected abstract void initMaps();
	
	protected abstract List<V> createList();
	
	@Override
	public final void collect(K key, V value)
	{
		int partitionId = partition.part(key,reducerCount);
		
		List<V> values = datas[partitionId].get(key);
		if(values!=null){
			values.add(value);
		}else{
			values = createList();
			values.add(value);
			datas[partitionId].put(key, values);
		}
	}
 
	@Override
	public final Map<K,List<V>> getResult(int reducerId) {
		return datas[reducerId];
	}

}
