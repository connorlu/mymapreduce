package org.itew.mymapreduce.core.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.itew.mymapreduce.core.BasicDataReader;

public class SortedDataReader<K, V> extends BasicDataReader<K, V> {
	
	
	private Map<Iterator<K>,K> mapIterators;
	
	private final int mapLength;
	
	private K currentKey;
	private List<V> currentValues;
	
	public SortedDataReader(Map<K, List<V>>[] maps) {
		super(maps);
		mapLength = maps.length;
		mapIterators = new LinkedHashMap<>(mapLength,0.75f,false);
		
		Iterator<K> iterator;
		for(int i=0;i<mapLength;i++){
			if(!maps[i].isEmpty()){
				iterator = maps[i].keySet().iterator();
				mapIterators.put(iterator,iterator.next());
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean next() throws IOException {
		
		Comparable<? super K> minKey = null;
		List<V> values = null;
		K currentKey = null;
		
		for(Iterator<Entry<Iterator<K>,K>> iterator = mapIterators.entrySet().iterator();iterator.hasNext();){
			currentKey = iterator.next().getValue();
			if(currentKey!=null){
				if(minKey!=null){
					if(minKey.compareTo(currentKey)>0){
						minKey = (Comparable<? super K>) currentKey;
					}
				}else{
					minKey = (Comparable<? super K>) currentKey;
				}
			}
		}
		
		if(minKey==null)
			return false;
		int i=0;
		Entry<Iterator<K>,K> currentEntry;
		Iterator<K> currentIterator;
		
		for(Iterator<Entry<Iterator<K>,K>> iterator = mapIterators.entrySet().iterator();iterator.hasNext();){
			currentEntry = iterator.next();
			currentKey = currentEntry.getValue();
			currentIterator = currentEntry.getKey();
			if(currentKey!=null&&minKey.compareTo(currentKey)==0){
				if(values==null)
					values = maps[i].get(currentKey);
				else
					values.addAll(maps[i].get(currentKey));
				
				
				if(currentIterator.hasNext())
					currentEntry.setValue(currentIterator.next());
				else
					currentEntry.setValue(null);
			}
			
			i++;
		}
		this.currentKey = (K)minKey;
		this.currentValues = values;
		return true;
	}

	@Override
	public K getKey() {
		return currentKey;
	}

	@Override
	public List<V> getValue() {
		return currentValues;
	}

}
