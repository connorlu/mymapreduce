package org.itew.mymapreduce.util;

import java.io.IOException;
import java.util.Properties;

public class PropertiesConfigurationReader {
	
	private final Properties properties;
	
	public PropertiesConfigurationReader(Class<?> useClass,String propertiesName) throws IOException
	{
		properties = new Properties();
		properties.load(useClass.getClassLoader().getResourceAsStream(propertiesName));
	}
	
	public String getValue(String key)
	{
		return properties.getProperty(key, null);
	}
	
	public int getInt(String key){
		return Integer.parseInt(properties.getProperty(key, null));
	}
	
	public long getLong(String key){
		return Long.parseLong(properties.getProperty(key, null));
	}
	
	public char getChar(String key){
		return properties.getProperty(key, null).charAt(0);
	}
	
}
