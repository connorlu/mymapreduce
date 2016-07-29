package org.itew.mymapreduce.core.impl;

import org.itew.mymapreduce.core.DataCollection;
import org.itew.mymapreduce.core.Mapper;

@SuppressWarnings("rawtypes")
public class SimpleMapper implements Mapper{
	@SuppressWarnings("unchecked")
	@Override
	public void map(Object key, Object value, DataCollection colletion) {
		colletion.collect(key, value);
	}
}
