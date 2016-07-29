package org.itew.mymapreduce.util;

import org.apache.log4j.Logger;



public class TimeUtil {
	
	public static void printNanoTime(long nanoTime)
	{
		int nano = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int micro = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int ms = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int s = (int) (nanoTime%60);
		nanoTime = nanoTime/60;
		int min = (int) (nanoTime%60);
		nanoTime = nanoTime/60;
		int h = (int) (nanoTime%24);
		nanoTime = nanoTime/24;
		int d = (int) nanoTime;
		
		System.out.println(d+"天"+h+"小时"+min+"分钟"+s+"秒"+ms+"毫秒"+micro+"微妙"+nano+"纳秒");
	}
	
	public static void printNanoTimeByLoggerInfo(Logger logger,String message,long nanoTime)
	{
		int nano = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int micro = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int ms = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int s = (int) (nanoTime%60);
		nanoTime = nanoTime/60;
		int min = (int) (nanoTime%60);
		nanoTime = nanoTime/60;
		int h = (int) (nanoTime%24);
		nanoTime = nanoTime/24;
		int d = (int) nanoTime;
		
		logger.info(message+":"+d+"天"+h+"小时"+min+"分钟"+s+"秒"+ms+"毫秒"+micro+"微妙"+nano+"纳秒");
	}
	
	public static void main(String[] args) throws InterruptedException {
//		printNanoTime(System.currentTimeMillis()*1000000);
		
		long start = System.nanoTime();
		
		Thread.sleep(1000*60);
		printNanoTime(System.nanoTime()-start);
		
	}

}
