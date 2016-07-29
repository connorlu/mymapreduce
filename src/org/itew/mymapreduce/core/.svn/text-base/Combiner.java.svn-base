package org.itew.mymapreduce.core;

import java.util.Collection;

public interface Combiner<K,V> {
	
	void combine(K key,Collection<V> values,DataCollection<K,V> outputCollection);
	
}
