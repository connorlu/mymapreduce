package org.itew.mymapreduce.core.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.itew.mymapreduce.core.BasicDataCollection;
import org.itew.mymapreduce.core.Configuration;
import org.itew.mymapreduce.core.Partition;

public class SortedDataCollection<K,V> extends BasicDataCollection<K,V>{

	public SortedDataCollection(Configuration configuration, Partition<K> partition) {
		super(configuration, partition);
	}

	@Override
	protected void initMaps() {
		for(int i=0;i<datas.length;i++){
			datas[i] = new TreeMap<K,List<V>>();
		}
	}

	@Override
	protected List<V> createList() {
		return new ArrayList<V>(1);
	}

	

}
