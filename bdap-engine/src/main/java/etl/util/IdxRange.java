package etl.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
}
