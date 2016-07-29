package org.itew.mymapreduce.io;


public abstract class OutputFormat<K,V> implements Writable<K,V>{
	
	protected final String path;
	
	public OutputFormat(String path){
		this.path = path;
	}

}
