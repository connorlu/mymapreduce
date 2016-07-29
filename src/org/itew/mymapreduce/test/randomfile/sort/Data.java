package org.itew.mymapreduce.test.randomfile.sort;


public class Data implements Comparable<Data> {

	public final int key;
	public final double value;
	
	public Data(int key,double value){
		this.key = key;
		this.value = value;
	}

	@Override
	public int compareTo(Data o) {
		return key > o.key ? 1 : key < o.key ? -1 : value > o.value ? 1 :value<o.value ? -1 : 0;
	}

	@Override
	public String toString() {
		return new StringBuilder(32).append(key).append('\t').append(value).toString();
	}

	@Override
	public int hashCode() {
		 long bits = Double.doubleToLongBits(value);
	     return (int)(bits ^ (bits >>> 17));
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof Data))
			return false;
		Data obj1 = (Data) obj;
		return key == obj1.key ? Double.compare(value, obj1.value)==0 ? true : false :false;
	}
	
	
	
}
