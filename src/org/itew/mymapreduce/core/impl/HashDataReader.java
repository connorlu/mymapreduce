package org.itew.mymapreduce.core.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.itew.mymapreduce.core.BasicDataReader;

public class HashDataReader<K, V> extends BasicDataReader<K, V> {
	
	private final Set<K> keySet; 
	private final Iterator<K> iterator;
	private K currentKey;
	
	public HashDataReader(Map<K, List<V>>[] maps) {
		super(maps);
		keySet = new HashSet<K>();
		for(int i=0;i<maps.length;i++){
			keySet.addAll(maps[i].keySet());
		}
		this.iterator = keySet.iterator();
	}

	@Override
	public boolean next() throws IOException {
		if(iterator.hasNext()){
			currentKey = iterator.next();
			return true;
		}
		return false;
	}

	@Override
	public K getKey() {
		return currentKey;
	}

	@Override
	public List<V> getValue() {
		
		List<V> values = null;
		
		for(int i=0;i<maps.length;i++){
			List<V> eachValues = maps[i].get(currentKey);
			if(eachValues!=null){
				if(values!=null){
					values.addAll(eachValues);
				}else{
					values = eachValues;
				}
			}
			
		}
		
		return values;
	}

}
