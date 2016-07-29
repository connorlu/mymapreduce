package org.itew.mymapreduce.test.randomfile.maxvalue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.itew.mymapreduce.util.TimeUtil;


public class MaxValueJobAdversary {
	
	public static void maxValueJovAdversary(String inputPath,String inputFile ,String outputPath,int bufferSize) throws NumberFormatException, IOException{
		
		long soloStart = System.nanoTime();
		
		double[] maxValue = new double[101];
		
		
		for(int i=1;i<maxValue.length;i++){
			maxValue[i] = Double.MIN_VALUE;
		}
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputPath+inputFile))),bufferSize);
		
		String line;
		while((line = br.readLine())!=null){
			String[] strs = line.split("@");
			int key = Integer.parseInt(strs[0]);
			double value = Double.parseDouble(strs[1]);
			if(maxValue[key]<value)
				maxValue[key]=value;
		}
		
		br.close();
		
		File outputDir = new File(inputPath+outputPath);
		outputDir.mkdirs();
		File outputFile = new File(inputPath+outputPath+"/output.txt");
		outputFile.createNewFile();
		
		PrintWriter pw = new PrintWriter(outputFile);
		
		for(int i=1;i<maxValue.length;i++){
			pw.println(i+"\t"+maxValue[i]);
		}

		pw.close();
		System.out.print("solo任务用时:");
		TimeUtil.printNanoTime(System.nanoTime()-soloStart);
	} 
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		 maxValueJovAdversary("E:/hadoop_test/a/","a.txt" ,"max-output-adversary", 40960);
	}
}
