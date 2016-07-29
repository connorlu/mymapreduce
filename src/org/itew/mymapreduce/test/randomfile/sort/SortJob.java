package org.itew.mymapreduce.test.randomfile.sort;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.itew.mymapreduce.core.BasicDataReader;
import org.itew.mymapreduce.core.Configuration;
import org.itew.mymapreduce.core.DataCollection;
import org.itew.mymapreduce.core.FrameworkDataCollection;
import org.itew.mymapreduce.core.Mapper;
import org.itew.mymapreduce.core.Partition;
import org.itew.mymapreduce.core.impl.HashDataCollection;
import org.itew.mymapreduce.core.impl.HashDataReader;
import org.itew.mymapreduce.core.impl.HashPartition;
import org.itew.mymapreduce.core.impl.JobBuilder;
import org.itew.mymapreduce.core.impl.SortedDataCollection;
import org.itew.mymapreduce.core.impl.SortedDataReader;
import org.itew.mymapreduce.io.data.Nothing;
import org.itew.mymapreduce.io.impl.TextInputFormat;
import org.itew.mymapreduce.io.impl.TextOutputFormat;

public class SortJob {
	
	public static class SortMapper implements Mapper<Long,String,Data,Nothing>{

		@Override
		public void map(Long key, String value,DataCollection<Data, Nothing> collection) {
			String[] strs = value.split("@");
			collection.collect(new Data(Integer.parseInt(strs[0]),Double.parseDouble(strs[1])), Nothing.INSTANCE);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public static void startJob(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<Long,String,Data,Nothing,Data,Nothing> jobBuilder = new JobBuilder<Long,String,Data,Nothing,Data,Nothing>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(TextInputFormat.class);
		
		jobBuilder.setOutputFormatClass((Class<? extends TextOutputFormat<Data, Nothing>>) TextOutputFormat.class);
		
		jobBuilder.setMapperClass(SortMapper.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Data, Nothing>>) SortedDataCollection.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Data>>) HashPartition.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Data, Nothing>>) SortedDataReader.class);
		
		jobBuilder.createJob().excute();
	}
	
	
	@SuppressWarnings("unchecked")
	public static void startJobHash(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<Long,String,Data,Nothing,Data,Nothing> jobBuilder = new JobBuilder<Long,String,Data,Nothing,Data,Nothing>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(TextInputFormat.class);
		
		jobBuilder.setOutputFormatClass((Class<? extends TextOutputFormat<Data, Nothing>>) TextOutputFormat.class);
		
		jobBuilder.setMapperClass(SortMapper.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Data, Nothing>>) HashDataCollection.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Data>>) HashPartition.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Data, Nothing>>) HashDataReader.class);
		
		jobBuilder.createJob().excute();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
//		PrintableSleep.sleep(15);
		startJob("E:/hadoop_test/a/","a.txt","sort-output-0.3");
		
//		startJobHash("E:/hadoop_test/a/","a.txt","sort-output-0.3");
	}

}
