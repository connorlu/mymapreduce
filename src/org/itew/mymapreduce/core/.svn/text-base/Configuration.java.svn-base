package org.itew.mymapreduce.core;

import java.io.IOException;

import org.itew.mymapreduce.util.PropertiesConfigurationReader;
/**
 * map reduce 配置文件
 * @author JiLu 
 * 
 */
public class Configuration {
	
	private int maxMapperCount;
	private int maxReducerCount;
	private int mapTaskMaxRunningTime;
	private int reduceTaskMaxRunningTime;

	public Configuration() throws IOException {
		
		
		PropertiesConfigurationReader reader = new PropertiesConfigurationReader(
				Configuration.class, "mymapreduce.properties");
		
		String automaticAllocateMapperCountAndReducerCountStr = reader.getValue("automaticAllocateMapperCountAndReducerCount");
		if(automaticAllocateMapperCountAndReducerCountStr!=null&&automaticAllocateMapperCountAndReducerCountStr.equals("true")){
			maxMapperCount = Runtime.getRuntime().availableProcessors();
			maxReducerCount = Runtime.getRuntime().availableProcessors();
		}else{
			maxMapperCount = reader.getInt("maxMapperCount");
			maxReducerCount = reader.getInt("maxReducerCount");;
		}
		
		mapTaskMaxRunningTime = reader.getInt("mapTaskMaxRunningTime");
		reduceTaskMaxRunningTime = reader.getInt("reduceTaskMaxRunningTime");
		
	}


	public int getMaxMapperCount() {
		return maxMapperCount;
	}

	public void setMaxMapperCount(int maxMapperCount) {
		this.maxMapperCount = maxMapperCount;
	}

	public int getMaxReducerCount() {
		return maxReducerCount;
	}

	public void setMaxReducerCount(int maxReducerCount) {
		this.maxReducerCount = maxReducerCount;
	}

	public int getMapTaskMaxRunningTime() {
		return mapTaskMaxRunningTime;
	}

	public void setMapTaskMaxRunningTime(int mapTaskMaxRunningTime) {
		this.mapTaskMaxRunningTime = mapTaskMaxRunningTime;
	}

	public int getReduceTaskMaxRunningTime() {
		return reduceTaskMaxRunningTime;
	}

	public void setReduceTaskMaxRunningTime(int reduceTaskMaxRunningTime) {
		this.reduceTaskMaxRunningTime = reduceTaskMaxRunningTime;
	}
	
}
