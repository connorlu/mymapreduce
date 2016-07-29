package org.itew.mymapreduce.test.randomfile.sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Set;
import java.util.TreeSet;

import org.itew.mymapreduce.util.PrintableSleep;
import org.itew.mymapreduce.util.TimeUtil;


public class SortJobAdversary {
	
	public static void sortJobAdversary(String inputPath ,String inputFile ,String outputPath,int bufferSize) throws NumberFormatException, IOException{
		
		long soloStart = System.nanoTime();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputPath+inputFile))),bufferSize);
		
		Set<Data> set = new TreeSet<Data>();
		
		String line;
		while((line = br.readLine())!=null){
			String[] strs = line.split("@");
			set.add(new Data(Integer.parseInt(strs[0]), Double.parseDouble(strs[1])));
		}
		
		br.close();
		
		System.out.print("文件读取+排序用时:");
		TimeUtil.printNanoTime(System.nanoTime()-soloStart);
		
		long writeTime = System.nanoTime();
				
		File outputDir = new File(inputPath+outputPath);
		outputDir.mkdirs();
		File outputFile = new File(inputPath+outputPath+"/output.txt");
		outputFile.createNewFile();
		
		PrintWriter pw = new PrintWriter(outputFile);
		
		for(Data data : set){
			pw.println(data);
		}
		
		pw.close();
		
		
		System.out.print("写文件用时:");
		TimeUtil.printNanoTime(System.nanoTime()-writeTime);
		System.out.print("solo任务用时:");
		TimeUtil.printNanoTime(System.nanoTime()-soloStart);
		
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		PrintableSleep.sleep(15);
		
		sortJobAdversary("E:/hadoop_test/a/","a.txt" ,"sort-output-adersary", 40960);
		
	}
	
}
