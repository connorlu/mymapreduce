package org.itew.mymapreduce.core.impl;

import org.itew.mymapreduce.core.Partition;


public class HashPartition<K> implements Partition<K> {

	@Override
	public int part(K key, int reduceCount) {
		return Math.abs(key.hashCode())%reduceCount;
	}

}
