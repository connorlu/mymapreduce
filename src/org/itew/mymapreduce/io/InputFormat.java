package org.itew.mymapreduce.io;

public abstract class InputFormat<K,V> implements Splittable<K,V>{
	
	protected final String path;
	
	public InputFormat(String path){
		this.path = path;
	}

}
