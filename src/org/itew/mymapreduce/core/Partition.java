package org.itew.mymapreduce.core;


public interface Partition<K> {
	
	int part(K key,int reduceCount);

}
