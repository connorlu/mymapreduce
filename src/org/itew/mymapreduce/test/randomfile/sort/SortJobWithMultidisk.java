package org.itew.mymapreduce.test.randomfile.sort;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.itew.mymapreduce.core.BasicDataReader;
import org.itew.mymapreduce.core.Configuration;
import org.itew.mymapreduce.core.DataCollection;
import org.itew.mymapreduce.core.FrameworkDataCollection;
import org.itew.mymapreduce.core.Mapper;
import org.itew.mymapreduce.core.Partition;
import org.itew.mymapreduce.core.Reducer;
import org.itew.mymapreduce.core.impl.HashPartition;
import org.itew.mymapreduce.core.impl.JobBuilder;
import org.itew.mymapreduce.core.impl.SortedDataCollection;
import org.itew.mymapreduce.core.impl.SortedDataReader;
import org.itew.mymapreduce.core.impl.StringReducer;
import org.itew.mymapreduce.io.data.Nothing;
import org.itew.mymapreduce.io.file.MultiDiskLineFile;

public class SortJobWithMultidisk {
	
	public static class SortMapper implements Mapper<String,String,Data,Nothing>{

		@Override
		public void map(String key, String value,DataCollection<Data, Nothing> collection) {
			collection.collect(new Data(Integer.parseInt(key),Double.parseDouble(value)), Nothing.INSTANCE);
		}
		
	}
	
	
	@SuppressWarnings("unchecked")
	public static void startJob(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<String,String,Data,Nothing,String,String> jobBuilder = new JobBuilder<String,String,Data,Nothing,String,String>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(MultiDiskLineFile.class);
		
		jobBuilder.setOutputFormatClass(MultiDiskLineFile.class);
		
		jobBuilder.setMapperClass(SortMapper.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Data, Nothing>>) SortedDataCollection.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Data>>) HashPartition.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Data, Nothing>>) SortedDataReader.class);
		
		jobBuilder.setReducerClass((Class<? extends Reducer<Data, Nothing, String, String>>) StringReducer.class);
		
		jobBuilder.createJob().excute();
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

//		PrintableSleep.sleep(15);
		
		startJob("/a/","a","sort");
		
	}

}
