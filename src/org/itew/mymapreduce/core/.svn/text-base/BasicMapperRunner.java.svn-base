package org.itew.mymapreduce.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.itew.mymapreduce.io.Readable;

/**
 * 该类是一个map任务的抽象，它负责接收一个分片并发本片传给用户编写的Map任务
 * @author JiLu
 * 
 */
public class BasicMapperRunner<IK,IV,OK,OV> implements Runnable{
	
	private final Mapper<IK,IV,OK,OV> mapper;
	private final FrameworkDataCollection<OK,OV> outCollecton;
	private final Combiner<OK,OV> combiner;
	private final int reducerCount;
	private final BlockingQueue<Readable<IK, IV>> workingQueue;
	private final Readable<IK,IV> deadPill;
	
	public BasicMapperRunner(Mapper<IK,IV,OK,OV> mapper,FrameworkDataCollection<OK,OV> outCollecton,Combiner<OK, OV> combiner,Configuration configuration,BlockingQueue<Readable<IK, IV>> workingQueue,Readable<IK,IV> deadPill)
	{
		this.mapper = mapper;
		this.outCollecton = outCollecton;
		this.combiner = combiner;
		this.reducerCount = configuration.getMaxReducerCount();
		this.workingQueue = workingQueue;
		this.deadPill = deadPill;
	}
	
	@Override
	public void run() {
		try {
			Readable<IK, IV> reader;
			
			while(true){

				reader = workingQueue.take();
				
				if(reader!=deadPill){
					while(reader.next())
						mapper.map(reader.getKey(), reader.getValue(), outCollecton);
				}else{
					workingQueue.put(deadPill);
					break;
				}
				
			}
			
			if(combiner!=null){
				
				for(int i=0;i<reducerCount;i++){
					
					Map<OK,List<OV>> map = outCollecton.getResult(i);
					
					for(OK key:map.keySet()){
						combiner.combine(key, map.get(key), outCollecton);
					}
				}
				
			}
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		
	}
	
}
