package org.itew.mymapreduce.io.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.itew.mymapreduce.io.InputFormat;
import org.itew.mymapreduce.io.Readable;
import org.itew.mymapreduce.util.PropertiesConfigurationReader;

/**
 * 这是一个 {@code InputFormat}的实现类，用于处理单个文本，该类会将文本按行分割，并读取configuration中的
 * 最大分片值，按照该值建立分片并返回每个分片的Readable对象。
 * @author JiLu
 *
 * @param <T> 这是一个<code>Readable<K,V></code>接口的子类型
 * @param <K> 可为任意确定对象类型的键，作为<code>Readable<K,V></code>的键(K)
 * @param <V> 可为任意确定对象类型的值，作为<code>Readable<K,V></code>的值(V)
 */
public class TextInputFormat extends InputFormat<Long, String> {
	
	private final File file;
	private final int bufferSize;
	private final Collection<TextReader> textReaders;
	private final int maxSplitoCount;
	
	public TextInputFormat(String path) throws IOException {
		
		super( path);
		
		this.file = new File(path);
		
		PropertiesConfigurationReader pcr = new PropertiesConfigurationReader(TextInputFormat.class, "textinputformat.properties");
		
		bufferSize = pcr.getInt("bufferSize");
		
		maxSplitoCount = pcr.getInt("maxSplitoCount");
		
		this.textReaders = new ArrayList<TextReader>(maxSplitoCount);
		
		final List<Long> perOffset = getEachSplitoOffset();
		
		for(int i=0;i<perOffset.size()-1;i++){
			TextReader textReader = new TextReader(file, perOffset.get(i), perOffset.get(i+1), bufferSize);
			textReaders.add(textReader);
		}
		
	}
	
	private List<Long> getEachSplitoOffset() throws IOException{

		long perSplitpSize = file.length()/maxSplitoCount +1;
		
		final List<Long> perOffset = new LinkedList<Long>(); 
		
		long currentOffset = 0;
		
		perOffset.add(0L);
		
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		
		try{
			
			for(long i = perSplitpSize;i<file.length();i += perSplitpSize)
			{
	
				 raf.seek(i);
				 
				 raf.readLine();
				 
				 currentOffset = raf.getFilePointer();
				 
				 raf.seek(currentOffset);
	
				 perOffset.add(currentOffset);
				 
				 while(currentOffset>i + perSplitpSize)
					 i += perSplitpSize;
			}
			
		}finally{
			raf.close();
		}
		
		perOffset.add(file.length());
		
		return perOffset;
	}

	private static class TextReader implements Readable<Long, String>{
		
		private final long endOffset;
		
		private final BufferedRandomAccessFileReader brafr;
		
		//line number
		private long key = 0;
		
		private String value = null;
		
		private TextReader(File file,long startOffset,long endOffset,int bufferSize) throws IOException{
			this.endOffset = endOffset;
			brafr = new BufferedRandomAccessFileReader(file, startOffset, bufferSize);
		}
		
		@Override
		public boolean next() throws IOException {
			try{
			if(brafr.getFilePointer()<endOffset){
				key++;
				value = brafr.readLine();
				return true;
			}
			brafr.close();
			return false;
			}catch(IOException e){
				brafr.close();
				throw e;
			}
		}

		@Override
		public Long getKey() {
			return key;
		}

		@Override
		public String getValue() {
			return value;
		}
		
	}

	@Override
	public int getFragmentCount() {
		return textReaders.size();
	}

	@Override
	public Collection<TextReader> getReaders() {
		return textReaders;
	}


}
