package etl.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVRecord;

public class IdxRange implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static final String SEP=";";
	public static final String TO="-";
	public static final int END=-1;
	
	private int start;
	private int end;
	
	
	public IdxRange(int idx){
		start = idx;
		end = idx;
	}
	
	public IdxRange(int start, int end){
		this.start = start;
		this.end = end;
	}
	
	@Override
	public String toString(){
		return String.format("%d:%d", start, end);
	}
	
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
	}
	
	public static List<IdxRange> parseString(String str){
		List<IdxRange> irList = new ArrayList<IdxRange>();
		if (str!=null){
			String[] irs = str.split(SEP);
			for (String ir:irs){
				if (ir.contains(TO)){
					String[] idxes = ir.split(TO);
					if (idxes.length==2){
						int start = Integer.parseInt(idxes[0]);
						int end = Integer.parseInt(idxes[1]);
						irList.add(new IdxRange(start, end));
					}else{
						int start = Integer.parseInt(idxes[0]);
						irList.add(new IdxRange(start, -1));
					}
				}else{
					int idx = Integer.parseInt(ir);
					irList.add(new IdxRange(idx));
				}
			}
		}
		return irList;
	}
	
	public static List<Integer> getIdxInRange(List<IdxRange> irl, int fieldNum){
		List<Integer> idxList = new ArrayList<Integer>();
		for (IdxRange ir: irl){
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd()==-1){
				end = fieldNum-1;
			}
			for (int i=start; i<=end; i++){
				idxList.add(i);
			}
		}
		return idxList;
	}
	
	public static List<Integer> getIdxOutRange(List<IdxRange> irl, int fieldNum){
		boolean[] idxMap=new boolean[fieldNum];//true skip
		for (IdxRange ir: irl){
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd()==-1){
				end = fieldNum-1;
			}
			for (int i=start; i<=end; i++){
				idxMap[i]=true;
			}
		}
		List<Integer> idxList = new ArrayList<Integer>();
		for (int i=0; i<fieldNum; i++){
			if (!idxMap[i]){
				idxList.add(i);
			}
		}
		return idxList;
	}
	
	public static List<String> getFieldsInRange(CSVRecord r, List<IdxRange> irl){
		List<String> keys = new ArrayList<String>();
		for (IdxRange ir: irl){
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd()==-1){
				end = r.size()-1;
			}
			for (int i=start; i<=end; i++){
				keys.add(r.get(i));
			}
		}
		return keys;
	}
	
	public static List<String> getFieldsOutRange(CSVRecord r, List<IdxRange> irl, int fieldNum){
		boolean[] idxMap=new boolean[fieldNum];//true skip
		for (IdxRange ir: irl){
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd()==-1){
				end = r.size()-1;
			}
			for (int i=start; i<=end; i++){
				idxMap[i]=true;
			}
		}
		List<String> values = new ArrayList<String>();
		for (int i=0; i<fieldNum; i++){
			if (!idxMap[i]){
				if (r!=null){
					values.add(r.get(i));
				}else{
					values.add("");
				}
			}
		}
		return values;
	}
}
