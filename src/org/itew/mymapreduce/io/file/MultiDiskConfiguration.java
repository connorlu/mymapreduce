package org.itew.mymapreduce.io.file;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.itew.mymapreduce.util.PropertiesConfigurationReader;

public class MultiDiskConfiguration {
	

	private final static PropertiesConfigurationReader reader = initReader();

	public final static int BLOCKSIZE = getBolckSize();
	public final static String[] PATHS =  getPaths();
	public final static String FILEPREFIX = getFilePrefix();
	public final static String FOLDERPREFIX = getFolderPrefix();

	private static PropertiesConfigurationReader initReader(){
		try {
			return new PropertiesConfigurationReader(
					MultiDiskConfiguration.class, "multidiskfile.properties");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static String getFilePrefix(){
		return reader.getValue("filePrefix");
	}
	
	private static String getFolderPrefix(){
		return reader.getValue("folderPrefix");
	}
	
	private static int getBolckSize(){
		
		return reader.getInt("blockSize") * 1024;
	}
	
	private static String[] getPaths() {

		String pathsStr = reader.getValue("fileFolder");

		StringTokenizer st = new StringTokenizer(pathsStr, ";");

		String[] paths = new String[st.countTokens()];

		int i = 0;
		File file;
		while (st.hasMoreTokens()){
			
			paths[i] = st.nextToken();
			
			file = new File(paths[i]);
			
			if(!file.exists())
				file.mkdirs();
			
			i++;
			
		}
		return paths;

	}

}
