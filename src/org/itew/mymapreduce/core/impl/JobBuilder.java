package org.itew.mymapreduce.core.impl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.itew.mymapreduce.core.BasicDataReader;
import org.itew.mymapreduce.core.BasicMapperRunner;
import org.itew.mymapreduce.core.BasicReducerRunner;
import org.itew.mymapreduce.core.Combiner;
import org.itew.mymapreduce.core.Configuration;
import org.itew.mymapreduce.core.FrameworkDataCollection;
import org.itew.mymapreduce.core.Job;
import org.itew.mymapreduce.core.Mapper;
import org.itew.mymapreduce.core.Partition;
import org.itew.mymapreduce.core.Reducer;
import org.itew.mymapreduce.io.Readable;
import org.itew.mymapreduce.io.Splittable;
import org.itew.mymapreduce.io.Writable;
import org.itew.mymapreduce.io.data.DeadReadable;
import org.itew.mymapreduce.io.impl.TextOutputFormat;
import org.itew.mymapreduce.util.TimeUtil;

public class JobBuilder<IK, IV, MK, MV, RK, RV> {

	private final static Logger logger = LogManager.getLogger(JobBuilder.class
			.getName());

	private Configuration configuration;

	private Class<? extends Mapper<IK, IV, MK, MV>> mapperClass;
	private Class<? extends Combiner<MK, MV>> combinerClass;
	private Class<? extends Partition<MK>> partitionClass;
	private Class<? extends Reducer<MK, MV, RK, RV>> reducerClass;

	private Class<? extends Splittable<IK, IV>> inputFormatClass;
	private Class<? extends Writable<RK, RV>> outputFormatClass;

	private Class<? extends FrameworkDataCollection<MK, MV>> dataCollectionClass;
	private Class<? extends BasicDataReader<MK, MV>> dataReaderClass;
	private Class<? extends BasicMapperRunner<IK, IV, MK, MV>> mapperRunnerClass;
	private Class<? extends BasicReducerRunner<MK, MV, RK, RV>> reducerRunnerClass;
	
	

	private String inputPath;

	private String outputPath;

	private final class SimpleJob implements Job {

		/**
		 * 计算分片
		 * @return 返回每个分片的阅读器
		 * @throws NoSuchMethodException
		 * @throws SecurityException
		 * @throws InstantiationException
		 * @throws IllegalAccessException
		 * @throws IllegalArgumentException
		 * @throws InvocationTargetException
		 */
		private Collection<? extends Readable<IK, IV>> computeSplito()
				throws NoSuchMethodException, SecurityException,
				InstantiationException, IllegalAccessException,
				IllegalArgumentException, InvocationTargetException {

			long computeSplitoStart = 0l;
			if (logger.isInfoEnabled())
				computeSplitoStart = System.nanoTime();

			Constructor<? extends Splittable<IK, IV>> inputFormatConstructor = inputFormatClass
					.getConstructor(String.class);

			Splittable<IK, IV> inputFormat = inputFormatConstructor
					.newInstance(inputPath);

			if (logger.isInfoEnabled())
				TimeUtil.printNanoTimeByLoggerInfo(logger, "计算分片用时:",
						System.nanoTime() - computeSplitoStart);

			// 返回所有的分片的阅读器
			return inputFormat.getReaders();

		}

		/**
		 * 执行map任务
		 * @param readers 每个分片的阅读器
		 * @return 返回map任务处理好的结果
		 * @throws InstantiationException
		 * @throws IllegalAccessException
		 * @throws NoSuchMethodException
		 * @throws SecurityException
		 * @throws IllegalArgumentException
		 * @throws InvocationTargetException
		 * @throws InterruptedException 
		 */
		private List<FrameworkDataCollection<MK, MV>> map(Collection<? extends Readable<IK, IV>> readers)
				throws InstantiationException, IllegalAccessException,
				NoSuchMethodException, SecurityException,
				IllegalArgumentException, InvocationTargetException, InterruptedException {

			// ///////////////////////////////////map任务开始/////////////////////////////////////////////////////////
			long mapStartTime = 0l;
			if (logger.isInfoEnabled())
				mapStartTime = System.nanoTime();

			ExecutorService mapperExecutor = Executors.newFixedThreadPool(configuration.getMaxMapperCount());

			// 的到mapper接口了一个实例
			Mapper<IK, IV, MK, MV> mapper = mapperClass.newInstance();
			
			// 储存中间结果集合
			List<FrameworkDataCollection<MK, MV>> mapOutputCollections = new ArrayList<FrameworkDataCollection<MK, MV>>(
					configuration.getMaxMapperCount());

			Constructor<? extends BasicMapperRunner<IK, IV, MK, MV>> mapperRunnerConstructor = mapperRunnerClass
					.getConstructor(Mapper.class,FrameworkDataCollection.class, Combiner.class,
							Configuration.class, BlockingQueue.class,Readable.class);
			
			Constructor<? extends FrameworkDataCollection<MK, MV>> frameworkDataCollectionConstructor = dataCollectionClass.getConstructor(Configuration.class,Partition.class);
			
			Partition<MK> partition = partitionClass.newInstance();
			
			//用于传输分片的工作队列
			BlockingQueue<Readable<IK, IV>> workingQueue = new LinkedBlockingQueue<Readable<IK, IV>>();
			
			Readable<IK, IV> deadPill = new DeadReadable<IK,IV>();
			
			Combiner<MK,MV> afterMappingCombiner = null;
			if (combinerClass != null)
				afterMappingCombiner=combinerClass.newInstance();
			
			for (int i=0;i<configuration.getMaxMapperCount();i++) {
				
				FrameworkDataCollection<MK, MV> mapOutputCollection = frameworkDataCollectionConstructor.newInstance(configuration,partition);
				
				mapOutputCollections.add(mapOutputCollection);
				
				mapperExecutor.execute(mapperRunnerConstructor.newInstance(mapper, mapOutputCollection, afterMappingCombiner,configuration,workingQueue,deadPill));
			}

			for(Readable<IK, IV> reader : readers){
				workingQueue.put(reader);
			}
			workingQueue.put(deadPill);
			
			mapperExecutor.shutdown();
			try {
				mapperExecutor.awaitTermination(
						configuration.getMapTaskMaxRunningTime(),
						TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error("map任务没有在"
						+ configuration.getMapTaskMaxRunningTime()
						+ "秒内完成程序异常终止");
				System.exit(0);
			}

			if (logger.isInfoEnabled())
				TimeUtil.printNanoTimeByLoggerInfo(logger, "计算Map任务用时:",
						System.nanoTime() - mapStartTime);

			return mapOutputCollections;

		}


		/**
		 * reduce任务执行
		 * 
		 * @param mappedCollections
		 *            经过map方法处理的结果集
		 * @param partedKeySets
		 *            经过partition方法处理后的，每个reduce任务的KeySet集合
		 * @throws InstantiationException
		 * @throws IllegalAccessException
		 * @throws NoSuchMethodException
		 * @throws SecurityException
		 * @throws IllegalArgumentException
		 * @throws InvocationTargetException
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		@SuppressWarnings("unchecked")
		private void reduce(List<FrameworkDataCollection<MK, MV>> mappedCollections)
				throws InstantiationException, IllegalAccessException,
				NoSuchMethodException, SecurityException,
				IllegalArgumentException, InvocationTargetException, InterruptedException, IOException {

			long reduceStartTime = 0l;
			if (logger.isInfoEnabled())
				reduceStartTime = System.nanoTime();
			
			if(outputFormatClass == TextOutputFormat.class){
				final File outputForlder = new File(outputPath);
				if (!outputForlder.exists())
					outputForlder.mkdirs();
			}else{
				
			}

			Constructor<? extends BasicDataReader<MK, MV>> dataReaderConstructor = dataReaderClass.getConstructor(Map[].class);
			
			Map<MK, List<MV>>[][] maps = new Map[configuration.getMaxReducerCount()][configuration.getMaxMapperCount()];
			
			for(int i=0;i<configuration.getMaxReducerCount();i++)
				for(int j=0;j<configuration.getMaxMapperCount();j++){
					maps[i][j] = mappedCollections.get(j).getResult(i);
				}
			
			mappedCollections.clear();
			mappedCollections=null;
			
			ExecutorService reduceExecutor = Executors.newFixedThreadPool(configuration.getMaxReducerCount());

			Reducer<MK, MV, RK, RV> reducer = reducerClass.newInstance();

			Constructor<? extends Writable<RK, RV>> outputFormatConstructor = outputFormatClass.getConstructor(String.class);

			Constructor<? extends BasicReducerRunner<MK, MV, RK, RV>> reducerRunnerConstructor = reducerRunnerClass.getConstructor(
					Readable.class, Reducer.class, Writable.class);
			

			
			for (int i=0;i<configuration.getMaxReducerCount();i++) {
				
				final String currentReduceTaskOutputPath = outputPath
						+ "/reduce-part-" + i;
				Writable<RK, RV> outputFormat = outputFormatConstructor
						.newInstance(currentReduceTaskOutputPath);
				BasicReducerRunner<MK, MV, RK, RV> reducerRunner = reducerRunnerConstructor
						.newInstance(dataReaderConstructor.newInstance((Object)maps[i]), reducer,
								outputFormat);
				reduceExecutor.execute(reducerRunner);
			}
			
			
			reduceExecutor.shutdown();
			try {
				reduceExecutor.awaitTermination(
						configuration.getReduceTaskMaxRunningTime(),
						TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error("reduce任务没有在"
						+ configuration.getReduceTaskMaxRunningTime()
						+ "秒内完成程序异常终止");
				System.exit(0);
			}

			if (logger.isInfoEnabled())
				TimeUtil.printNanoTimeByLoggerInfo(logger, "reduce任务用时:",
						System.nanoTime() - reduceStartTime);
		}

		@Override
		public void excute() throws NoSuchMethodException, SecurityException,
				IOException, InstantiationException, IllegalAccessException,
				IllegalArgumentException, InvocationTargetException,
				InterruptedException {

			long jobStartTime = System.nanoTime();

			reduce(map(computeSplito()));

			TimeUtil.printNanoTimeByLoggerInfo(logger, "作业运行总时间：",System.nanoTime() - jobStartTime);
		}

	}

	public JobBuilder(Configuration configuration) throws IOException {

		this.configuration = configuration;

		logger.info("MaxMapperCount:" 
				+ configuration.getMaxMapperCount());
		logger.info("MaxReducerCount:" 
				+ configuration.getMaxReducerCount());
		logger.info("mapTaskMaxRunningTime:"
				+ configuration.getMapTaskMaxRunningTime() + "s");
		logger.info("reduceTaskMaxRunningTime:"
				+ configuration.getReduceTaskMaxRunningTime() + "s");

	}

	@SuppressWarnings("unchecked")
	public Job createJob() {
		if (inputPath == null)
			throw new NullPointerException("inputPath should not null.");
		if (outputPath == null)
			throw new NullPointerException("outputPath should not null.");
		if (inputFormatClass == null)
			throw new NullPointerException("inputFormatClass should not null.");
		if (outputFormatClass == null)
			throw new NullPointerException("outputFormatClass should not null.");

		if (mapperClass == null)
			mapperClass = (Class<? extends Mapper<IK, IV, MK, MV>>) SimpleMapper.class;
		if (partitionClass == null)
			partitionClass = (Class<? extends Partition<MK>>) HashPartition.class;
		if (reducerClass == null)
			reducerClass = (Class<? extends Reducer<MK, MV, RK, RV>>) SimpleReducer.class;

		if (dataCollectionClass == null)
			dataCollectionClass = (Class<? extends FrameworkDataCollection<MK, MV>>) HashDataCollection.class;
		if(dataReaderClass==null)
			dataReaderClass = (Class<? extends BasicDataReader<MK, MV>>) HashDataReader.class;
		if (mapperRunnerClass == null)
			mapperRunnerClass = (Class<? extends BasicMapperRunner<IK, IV, MK, MV>>) BasicMapperRunner.class;
		if (reducerRunnerClass == null)
			reducerRunnerClass = (Class<? extends BasicReducerRunner<MK, MV, RK, RV>>) BasicReducerRunner.class;

		return new SimpleJob();
	}

	public Class<? extends Mapper<IK, IV, MK, MV>> getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(
			Class<? extends Mapper<IK, IV, MK, MV>> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public Class<? extends Combiner<MK, MV>> getCombinerClass() {
		return combinerClass;
	}

	public void setCombinerClass(
			Class<? extends Combiner<MK, MV>> afterMappingCombinerClass) {
		this.combinerClass = afterMappingCombinerClass;
	}


	public Class<? extends Partition<MK>> getPartitionClass() {
		return partitionClass;
	}

	public void setPartitionClass(Class<? extends Partition<MK>> partitionClass) {
		this.partitionClass = partitionClass;
	}

	public Class<? extends Reducer<MK, MV, RK, RV>> getReducerClass() {
		return reducerClass;
	}

	public void setReducerClass(
			Class<? extends Reducer<MK, MV, RK, RV>> reducerClass) {
		this.reducerClass = reducerClass;
	}

	
	public Class<? extends BasicDataReader<MK, MV>> getDataReaderClass() {
		return dataReaderClass;
	}

	public void setDataReaderClass(
			Class<? extends BasicDataReader<MK, MV>> dataReaderClass) {
		this.dataReaderClass = dataReaderClass;
	}

	public Class<? extends FrameworkDataCollection<MK, MV>> getDataCollectionClass() {
		return dataCollectionClass;
	}

	public void setDataCollectionClass(
			Class<? extends FrameworkDataCollection<MK, MV>> dataCollectionClass) {
		this.dataCollectionClass = dataCollectionClass;
	}

	public Class<? extends BasicMapperRunner<IK, IV, MK, MV>> getMapperRunnerClass() {
		return mapperRunnerClass;
	}

	public void setMapperRunnerClass(
			Class<? extends BasicMapperRunner<IK, IV, MK, MV>> mapperRunnerClass) {
		this.mapperRunnerClass = mapperRunnerClass;
	}

	public Class<? extends BasicReducerRunner<MK, MV, RK, RV>> getReducerRunnerClass() {
		return reducerRunnerClass;
	}

	public void setReducerRunnerClass(
			Class<? extends BasicReducerRunner<MK, MV, RK, RV>> reducerRunnerClass) {
		this.reducerRunnerClass = reducerRunnerClass;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public Class<? extends Splittable<IK, IV>> getInputFormatClass() {
		return inputFormatClass;
	}

	public void setInputFormatClass(
			Class<? extends Splittable<IK, IV>> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}

	public Class<? extends Writable<RK, RV>> getOutputFormatClass() {
		return outputFormatClass;
	}

	public void setOutputFormatClass(
			Class<? extends Writable<RK, RV>> outputFormatClass) {
		this.outputFormatClass = outputFormatClass;
	}

}
