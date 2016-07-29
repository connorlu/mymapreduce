package org.itew.mymapreduce.io.impl;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
/**
 * 该类是一个带缓冲的随机文件读取器
 * @author JiLu
 *
 */
public class BufferedRandomAccessFileReader {

	private final RandomAccessFile randomAccessFile;
	
	private final byte[] buffer;
	
	private long bufferStart;
	
	private int bufferOffset;
	
	private int bufferEnd;
	
	private final int bufferSize;
	
	public BufferedRandomAccessFileReader(File file,long offset,int bufferSize) throws IOException
	{
		randomAccessFile = new RandomAccessFile(file, "r");
		this.bufferSize = bufferSize;
		buffer = new byte[bufferSize];
		randomAccessFile.seek(offset);
		bufferEnd = randomAccessFile.read(buffer,0,bufferSize);
		if(bufferEnd<1)
			throw new EOFException("file.length less than offset");
		bufferOffset = 0;
		bufferStart = offset;
	}
	
	public void close() throws IOException
	{
		randomAccessFile.close();
	}
	
	public long getFilePointer(){
		return bufferStart+bufferOffset;
	}
	
	private void loadFromFile() throws IOException{
		bufferStart += bufferSize;
		bufferOffset = 0;
		bufferEnd = randomAccessFile.read(buffer,0,bufferSize);
		if(bufferEnd<1)
			throw new EOFException("file.length less than offset");
	}
	
	private int read() throws IOException{
		if(bufferOffset<bufferEnd)
		{
			return buffer[bufferOffset++];
		}else if(bufferEnd==bufferSize){
			loadFromFile();
			return buffer[bufferOffset++];
		}else{
			return -1;
		}
	}
	
	
	
	public final String readLine() throws IOException {
        StringBuilder input = new StringBuilder();
        int c = -1;
        boolean endOfLine = false;

        while (!endOfLine) {
            switch (c = read()) {
            case -1:
            case '\n':
                endOfLine = true;
                break;
            case '\r':
                break;
            default:
                input.append((char)c);
                break;
            }
        }

        if ((c == -1) && (input.length() == 0)) {
            return null;
        }
        
        return input.toString();
    }

}