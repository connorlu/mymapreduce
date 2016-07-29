package org.itew.mymapreduce.test.multidiskfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.itew.mymapreduce.util.TimeUtil;

public class MultiDiskFileCopyAdversary {

	public static void copyFile(String fromPath,String toPath,int bufferSize) throws IOException {

		long start = System.nanoTime();

		final File fromFile = new File(fromPath);
		
		final File toFile = new File(toPath);

		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(fromFile)),bufferSize);

		PrintWriter pw = new PrintWriter(toFile);
		
		toFile.createNewFile();
		
		try {
			String str = null;
			int i = 0;
			while ((str = br.readLine()) != null) {
				i++;
				pw.println(i+"@"+str);
			}
			
			System.out.println(i);
		} finally {
			pw.close();
			br.close();
		}

		System.out.print("创建测试文件用时:");
		TimeUtil.printNanoTime(System.nanoTime() - start);
	}
	

	public static void main(String[] args) throws IOException, InterruptedException {
		copyFile("E:/hadoop_test/a/a.txt","E:/hadoop_test/a/a(2).txt",4096);
	}
}
