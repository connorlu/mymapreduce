package org.itew.mymapreduce.io.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.itew.mymapreduce.io.OutputFormat;
import org.itew.mymapreduce.util.PropertiesConfigurationReader;


public class TextOutputFormat<K,V> extends OutputFormat<K,V>{
	
	private final PrintWriter pw;
	
	private final String keyValueSeparator;

	public TextOutputFormat(String path) throws IOException {
		super(path);
		File file = new File(path);
		if(!file.getParentFile().exists())
			file.getParentFile().mkdirs();
		file.createNewFile();
		pw = new PrintWriter(file);
		PropertiesConfigurationReader reader = new PropertiesConfigurationReader(TextOutputFormat.class, "textoutputformat.properties");
		keyValueSeparator = reader.getValue("keyValueSeparator");
	}

	@Override
	public void write(K key, V value) {
		pw.println(key+keyValueSeparator+value);
	}

	@Override
	public void close() {
		pw.close();
	}

}
