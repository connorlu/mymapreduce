package org.itew.mymapreduce.test.randomfile;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import org.itew.mymapreduce.util.TimeUtil;

public class RandomFileCreator {
	
	public static void createFile(String path,int maxLine) throws IOException{
		
		long start = System.nanoTime();
		
		Random random = new Random();
		
		final File file = new File(path);
		
		file.createNewFile();
		
		PrintWriter pw = new PrintWriter(file);
		try{
			for(int i=0;i<maxLine;i++){
				pw.println(random.nextInt(100)+1+"@"+Math.random());
			}
		}finally{
			pw.close();
		} 
		
		System.out.print("创建测试文件用时:");
		TimeUtil.printNanoTime(System.nanoTime()-start);
	}
	
	public static void main(String[] args) throws IOException {
		
		 createFile("E:/hadoop_test/a/a.txt",5000000);
		 
		 createFile("E:/hadoop_test/b/b.txt",1000000);
		 
		 createFile("E:/hadoop_test/c/c.txt",1000);
	}
}
