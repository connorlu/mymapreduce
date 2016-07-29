package org.itew.mymapreduce.core.impl;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.itew.mymapreduce.core.Reducer;
import org.itew.mymapreduce.io.Writable;

@SuppressWarnings("rawtypes")
public class SimpleReducer implements Reducer{

	@SuppressWarnings("unchecked")
	@Override
	public void reduce(Object key, Iterable values,
			Writable writer) throws FileNotFoundException, IOException {
		try{
		writer.write(key, values.iterator().next());
		}catch(NullPointerException r){
			System.out.println(key +"  "+values+"  ");
		}
		
	}
	
}
