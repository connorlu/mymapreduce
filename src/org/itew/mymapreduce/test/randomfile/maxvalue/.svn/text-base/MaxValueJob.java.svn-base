package org.itew.mymapreduce.test.randomfile.maxvalue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.itew.mymapreduce.core.BasicDataReader;
import org.itew.mymapreduce.core.Configuration;
import org.itew.mymapreduce.core.DataCollection;
import org.itew.mymapreduce.core.FrameworkDataCollection;
import org.itew.mymapreduce.core.Mapper;
import org.itew.mymapreduce.core.Partition;
import org.itew.mymapreduce.core.Reducer;
import org.itew.mymapreduce.core.impl.HashDataCollection;
import org.itew.mymapreduce.core.impl.HashDataReader;
import org.itew.mymapreduce.core.impl.HashPartition;
import org.itew.mymapreduce.core.impl.JobBuilder;
import org.itew.mymapreduce.core.impl.SortedDataCollection;
import org.itew.mymapreduce.core.impl.SortedDataReader;
import org.itew.mymapreduce.io.Writable;
import org.itew.mymapreduce.io.impl.TextInputFormat;
import org.itew.mymapreduce.io.impl.TextOutputFormat;


public class MaxValueJob {
	
	public static class MaxValueMapper implements Mapper<Long,String,Integer,Double>{

		@Override
		public void map(Long key, String value,DataCollection<Integer, Double> collection) {
			
			String[] strs = value.split("@");
			collection.collect(Integer.parseInt(strs[0]), Double.parseDouble(strs[1]));
			
		}
		
	}
	
	public static class MaxValueReducer implements Reducer<Integer,Double,Integer,Double>{

		@Override
		public void reduce(Integer key, Iterable<Double> values,Writable<Integer, Double> writer) throws IOException {
			
			double maxValue = Double.MIN_VALUE;
			
			for(double value:values){
				if(value>maxValue)
					maxValue=value;
			}
			
			writer.write(key, maxValue);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public static void startJob(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<Long,String,Integer,Double,Integer,Double> jobBuilder = new JobBuilder<Long,String,Integer,Double,Integer,Double>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(TextInputFormat.class);
		
		jobBuilder.setOutputFormatClass((Class<? extends TextOutputFormat<Integer, Double>>) TextOutputFormat.class);
		
		jobBuilder.setMapperClass(MaxValueMapper.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Integer>>) HashPartition.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Integer, Double>>) SortedDataCollection.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Integer, Double>>) SortedDataReader.class);
		
		jobBuilder.setReducerClass(MaxValueReducer.class);
		
		jobBuilder.createJob().excute();
	}
	
	@SuppressWarnings("unchecked")
	public static void startJobHash(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<Long,String,Integer,Double,Integer,Double> jobBuilder = new JobBuilder<Long,String,Integer,Double,Integer,Double>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(TextInputFormat.class);
		
		jobBuilder.setOutputFormatClass((Class<? extends TextOutputFormat<Integer, Double>>) TextOutputFormat.class);
		
		jobBuilder.setMapperClass(MaxValueMapper.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Integer>>) HashPartition.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Integer, Double>>) HashDataCollection.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Integer, Double>>) HashDataReader.class);
		
		jobBuilder.setReducerClass(MaxValueReducer.class);
		
		jobBuilder.createJob().excute();
	}
	

	public static void main(String[] args) throws IOException, InterruptedException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
//		startJob("E:/hadoop_test/a/","a.txt","max-output-0.3");
		
		startJobHash("E:/hadoop_test/a/","a.txt","max-output-0.3");
	}
}
