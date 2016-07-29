package org.itew.mymapreduce.io.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.itew.mymapreduce.io.Readable;
import org.itew.mymapreduce.util.PropertiesConfigurationReader;

public class MultiDiskLineFile extends MultiDiskFile<String,String>{
	
	private static final int bufferSize = getBufferSize();
	private static final char lineToken = getLineToken();
	
	private File currentBlockItem;
	private long currentBlockSize;
	private PrintWriter pw;
	private StringBuilder sb;
	
	private static int getBufferSize(){
		try {
			PropertiesConfigurationReader reader = new PropertiesConfigurationReader(MultiDiskLineFile.class, "multidisklinefile.properties");
			return reader.getInt("bufferSize");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static char getLineToken(){
		try {
			PropertiesConfigurationReader reader = new PropertiesConfigurationReader(MultiDiskLineFile.class, "multidisklinefile.properties");
			return reader.getChar("lineToken");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	

	public MultiDiskLineFile(String path)throws IOException {
		super(path, true);
		
		if(!blockItems.isEmpty()){
			currentBlockItem = blockItems.get(blockItems.size());
			currentBlockSize = currentBlockItem.length();
		}
		
		sb = new StringBuilder();
	}
	
	public MultiDiskFile<String,String>[] listChild() {
		throw new UnsupportedOperationException("MultiDiskLineFile类不支持listChildPath方法，因为该类都指向某一个文件，所以没有子文件.");
	}
	
	@Override
	public void close() {
		if(pw!=null)
			pw.close();
	}
	
	@Override
	public void write(String key, String value) throws IOException {
		
		if(currentBlockItem!=null&&currentBlockSize<BLOCKSIZE){
			
			if(pw==null)
				pw = new PrintWriter(currentBlockItem);
			
		}else{
			if(!isExists)
				createFileORDir();
			
			if(pw!=null)
				pw.close();
			
			int fileId = blockItems.size()+1;
			
			int folderNum = (fileId-1)%folderItems.size();
			
			currentBlockItem = new File(sb.append(folderItems.get(folderNum).getPath()).append(File.separator).append(fileId).toString());
			currentBlockSize = 0l;
			logger.debug("create new block"+sb.toString());
			
			sb.delete(0, sb.length());
			
			currentBlockItem.createNewFile();
			
			pw = new PrintWriter(currentBlockItem);
			
			blockItems.put(fileId, currentBlockItem);
			
		}
		
		pw.print(sb.append(key).append(lineToken).append(value).append('\n').toString());
		currentBlockSize += sb.length();
		sb.delete(0, sb.length());
	}

	@Override
	public Collection<? extends Readable<String, String>> getReaders() {
		
		List<LineReader> lineReaders = new ArrayList<LineReader>(getFragmentCount());
		try {
			for(Entry<Integer, File> entry: blockItems.entrySet()){
					lineReaders.add(new LineReader(entry.getValue()));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return lineReaders;
	}
	
	private static class LineReader implements Readable<String, String>{
		
		private final BufferedReader bufferReader;
		
		private String key = null;
		
		private String value = null;
		
		private LineReader(File file) throws IOException{
			bufferReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)),bufferSize);
		}
		
		@Override
		public boolean next() throws IOException {
			String str = bufferReader.readLine();
			if(str==null)
				return false;
			int tokenIndex = str.indexOf(lineToken);
			if(tokenIndex==-1)
				return false;
			
			key = str.substring(0, tokenIndex);
			
			value = str.substring(tokenIndex+1);
			
			return true;
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public String getValue() {
			return value;
		}
		
	}
	

}
