package org.itew.mymapreduce.test.multidiskfile;

import java.io.IOException;

import org.itew.mymapreduce.io.file.MultiDiskFile;
import org.itew.mymapreduce.util.TimeUtil;

public class MultiDiskFileFun {

	@SuppressWarnings("rawtypes")
	public static void deleteFile(String path,boolean isFile) throws IOException {

		long start = System.nanoTime();

		MultiDiskFile<?,?> mdlf = new MultiDiskFile(path,isFile);

		if (mdlf.exists()) {
			if (mdlf.isDirectory())
				for (MultiDiskFile<?,?> file : mdlf.listChild()) {
					
					if (file.exists()){
						file.delete();
						System.out.println("delete file:"+file.getPath());
					}else{
						System.out.println("can not find file:"+file.getPath());
					}
				}

			mdlf.delete();
		}

		System.out.print("删除测试文件用时:");
		TimeUtil.printNanoTime(System.nanoTime() - start);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
//		deleteFile("/a/a",true);
		
		deleteFile("/a/sort",false);
	}
}
