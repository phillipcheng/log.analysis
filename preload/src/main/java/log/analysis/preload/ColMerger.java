package log.analysis.preload;

import java.util.TreeMap;

import org.apache.log4j.Logger;

public class ColMerger {
	public static final Logger logger = Logger.getLogger(ColMerger.class);

	public static final String COMMAND="remove.idx";
	
	int size;
	int currentSize;
	int[] orgidx; //the idx-es to wait
	TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
	String joiner;
	
	public static final String INIT_NULL="NULL";
	
	public ColMerger(int[] idx, String joiner){
		size = idx.length;
		orgidx = idx;
		this.joiner = joiner;
		reinit();
	}
	
	public void reinit(){
		for (int i=0; i<orgidx.length; i++){
			treeMap.put(orgidx[i], INIT_NULL);
		}
		currentSize=0;
	}
	
	public boolean contains(int idx){
		return treeMap.containsKey(idx);
	}
	
	public boolean add(int idx, String value){
		if (!treeMap.containsKey(idx)){
			logger.error("merger does not need idx:" + idx);
			return false;
		}
		String curValue = treeMap.get(idx);
		if (INIT_NULL.equals(curValue)){
			treeMap.put(idx, value);
			currentSize++;
		}else{ 
			logger.error(String.format("merger already has idx:%d with value:%s", idx, value));
		}
		
		if(currentSize==size){
			return true;
		}else{
			return false;
		}
	}
	
	public String getValue(){
		String output="";
		int i=0;
		for (int key:treeMap.keySet()){
			String val = treeMap.get(key);
			output+=val;
			if (i<currentSize-1){
				output+=joiner;
			}
			i++;
		}
		if (currentSize<size){
			logger.error(String.format("merger not completed yet expected size:%d, current size:%d", size, currentSize));
		}
		return output;
	}
}
