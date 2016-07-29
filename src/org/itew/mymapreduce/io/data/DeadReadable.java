package org.itew.mymapreduce.io.data;

import java.io.IOException;

import org.itew.mymapreduce.io.Readable;

public class DeadReadable<K,V> implements Readable<K, V> {

	@Override
	public boolean next() throws IOException {
		return false;
	}

	@Override
	public K getKey() {
		return null;
	}

	@Override
	public V getValue() {
		return null;
	}

}
