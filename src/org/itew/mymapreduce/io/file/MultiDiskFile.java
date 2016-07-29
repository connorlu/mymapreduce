package org.itew.mymapreduce.io.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.itew.mymapreduce.io.Readable;
import org.itew.mymapreduce.io.Splittable;
import org.itew.mymapreduce.io.Writable;

public class MultiDiskFile<K,V> implements Splittable<K,V>,Writable<K,V>{
	
	protected static final Logger logger = LogManager.getLogger(MultiDiskFile.class.getName());
	
	protected static final String[] PARENTPATHS = MultiDiskConfiguration.PATHS;

	protected static final int BLOCKSIZE = MultiDiskConfiguration.BLOCKSIZE;

	protected static final int PARENTPATHSLENGTH = MultiDiskConfiguration.PATHS.length;

	protected static final String FOLDERPREFIX = MultiDiskConfiguration.FOLDERPREFIX;

	protected static final String FILEPREFIX = MultiDiskConfiguration.FILEPREFIX;
	
	public static final MultiDiskFile<Object,Object> ROOT = new MultiDiskFile<Object,Object>();

	protected final String middlePath;

	protected final String name;

	protected final String path;

	protected boolean isFile;

	protected boolean isDirectory;

	protected boolean isExists;

	protected List<File> folderItems;

	protected Map<Integer,File> blockItems;

	private MultiDiskFile() {
		
		// 根目录 root
		this.isFile = false;
		this.isDirectory = true;
		this.isExists = true;
		this.path = "";
		this.middlePath = "/";
		this.name = "";

		folderItems = new ArrayList<File>(PARENTPATHSLENGTH);
		
		for (int i = 0; i < PARENTPATHSLENGTH; i++) {
			folderItems.add(new File(PARENTPATHS[i]));
		}
		
	}

	public MultiDiskFile(String path, boolean isFile)
			throws IOException {

		
		this.path = path.endsWith("/") ? path.substring(0, path.length()-1) : path;
		this.blockItems = new TreeMap<Integer,File>();

		if (path == null || path.length() <= 1)
			throw new FileNotFoundException();

		StringBuilder sb = new StringBuilder(path.length() * 3);

		String[] pathSplitos = path.split("/");
		
		List<String> pathRealSplitos = new ArrayList<String>(pathSplitos.length);

		sb.append("/");
		
		for (int i = 0; i < pathSplitos.length; i++) {
			
			if(pathSplitos[i]!=null&&!"".equals(pathSplitos[i].trim())){
				pathRealSplitos.add(pathSplitos[i].trim());
			}
		}
		
		for (int i = 0; i < pathRealSplitos.size()-1; i++) {
			sb.append(FOLDERPREFIX).append(pathRealSplitos.get(i)).append("/");
		}

		if (isFile) {
			this.isFile = true;
			this.isDirectory = false;
			sb.append(FILEPREFIX).append(pathRealSplitos.get(pathRealSplitos.size()-1));
			

			this.middlePath = sb.toString();
			this.name = pathRealSplitos.get(pathRealSplitos.size()-1);
			
			folderItems = new ArrayList<File>(PARENTPATHSLENGTH);
			for (int i = 0; i < PARENTPATHSLENGTH; i++) {
				folderItems.add(new File(PARENTPATHS[i] + this.middlePath));
			}

			if (folderItems.get(0).exists()) {
				isExists = true;
				for (int i = 0; i < PARENTPATHSLENGTH; i++) {
					for (File file : folderItems.get(i).listFiles()) {
						blockItems.put(Integer.valueOf(file.getName()),file);
					}
				}
			}

		} else {
			this.isFile = false;
			this.isDirectory = true;
			sb.append(FOLDERPREFIX).append(pathRealSplitos.get(pathRealSplitos.size()-1));
			

			this.middlePath = sb.toString();
			this.name = pathRealSplitos.get(pathRealSplitos.size()-1);
			
			folderItems = new ArrayList<File>(PARENTPATHSLENGTH);
			for (int i = 0; i < PARENTPATHSLENGTH; i++) {
				folderItems.add(new File(PARENTPATHS[i] + this.middlePath));
			}
			
			if (folderItems.get(0).exists()) 
				isExists = true;

		}

	}
	
	public final String getPath(){
		return path;
	}

	public final String getName() {
		return name;
	}

	public final boolean exists() {
		return isExists;
	}

	public final boolean isDirectory() {
		return isDirectory;
	}

	public final boolean isFile() {
		return isFile;
	}

	public final long length() {
		if(isDirectory)
			return 0l;
		
		long length = 0l;

		for (Entry<Integer, File> entry : blockItems.entrySet()) {
			length += entry.getValue().length();
		}
		return length;
	}

	public final boolean createFileORDir() throws IOException {
		if (isExists) {
			return false;
		}

		for (File file : folderItems) {
			file.mkdirs();
		}

		return true;
	}

	public final boolean delete() {

		for (Entry<Integer, File> entry : blockItems.entrySet()) {
			if (!entry.getValue().delete())
				return false;
		}
		blockItems.clear();
		for (File file : folderItems) {
			if (!file.delete())
				return false;
		}
		folderItems.clear();
		isExists = false;
		isFile = false;
		isDirectory = false;
		return true;
	}

	@SuppressWarnings("rawtypes")
	public MultiDiskFile[] listChild() {
		
		if(isFile)
			return null;
		
		File[] listFiles = folderItems.get(0).listFiles();
		
		List<MultiDiskFile> listChildPath = new ArrayList<MultiDiskFile>(listFiles.length);
		
		StringBuilder sb = new StringBuilder();
		try {
		
			for(File file : listFiles){
				if(file.getName().startsWith(FILEPREFIX)){
					listChildPath.add(new MultiDiskFile(sb.append(path).append("/").append(file.getName().substring(FILEPREFIX.length())).toString(),true));
					sb.delete(0, sb.length());
				}else{
					listChildPath.add(new MultiDiskFile(sb.append(path).append("/").append(file.getName().substring(FOLDERPREFIX.length())).toString(),false));
					sb.delete(0, sb.length());
				}
				
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return listChildPath.toArray(new MultiDiskFile[1]);
	}


	@Override
	public void close() {
		throw new UnsupportedOperationException("MultiDiskFile类不支持close方法，请选择适当的子类支持接方法.");
	}
	
	@Override
	public void write(K key, V value) throws FileNotFoundException, IOException {
		throw new UnsupportedOperationException("MultiDiskFile类不支持write方法，请选择适当的子类支持接方法.");
	}

	@Override
	public final int getFragmentCount() {
		return blockItems.size();
	}

	@Override
	public Collection<? extends Readable<K, V>> getReaders() {
		throw new UnsupportedOperationException("MultiDiskFile类不支持getReaders方法，请选择适当的子类支持接方法.");
	}

	
//	public static void main(String[] args) throws IOException {
//		
//		MultiDiskFile mdf = new MultiDiskFile("/a",false);
//		
//		System.out.println(mdf.createFileORDir());
//		
//		MultiDiskFile file1 = new MultiDiskFile("/a/b",true);
//		
//		System.out.println(file1.createFileORDir());
//		
//		for(MultiDiskFile m : MultiDiskFile.ROOT.listChildPath()){
//			System.out.println(m.getName() +" "+m.getPath()+" "+m.exists()+" "+m.isFile()+" "+m.isDirectory());
//			if(m.isDirectory()&&m.exists()){
//				for(MultiDiskFile m2 : m.listChildPath()){
//					System.out.println(m2.getName() +" "+m2.getPath()+" "+m2.exists()+" "+m2.isFile()+" "+m2.isDirectory());
//				}
//			}
//		}
//		
//		
//	}

}
