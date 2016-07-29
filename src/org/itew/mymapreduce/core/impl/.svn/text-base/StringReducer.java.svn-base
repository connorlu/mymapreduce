package org.itew.mymapreduce.core.impl;

import java.io.IOException;

import org.itew.mymapreduce.core.Reducer;
import org.itew.mymapreduce.io.Writable;

public class StringReducer<K,V> implements Reducer<K,V,String,String>{


	@Override
	public void reduce(K key, Iterable<V> values,
			Writable<String, String> writer) throws IOException {
		
		for(V value : values)
			writer.write(key.toString(), value.toString());
		
	}
	
}
