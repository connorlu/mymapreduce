package org.itew.mymapreduce.util;

import org.apache.log4j.Logger;

public class PrintableSleep {
	
	public static void sleep(int seconds){
		for(int i=seconds;i>=0;i--){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("还剩"+i+"秒");
		}
	}
	
	public static void sleepByLoggerInfo(Logger logger,int seconds)
	{
		for(int i=seconds;i>=0;i--){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("还剩"+i+"秒");
		}
	}

}
