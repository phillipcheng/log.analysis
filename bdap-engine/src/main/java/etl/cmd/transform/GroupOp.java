package etl.cmd.transform;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.commons.csv.CSVRecord;

import etl.engine.ETLCmd;
import etl.util.IdxRange;
import etl.util.ScriptEngineUtil;

public class GroupOp implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private String[] expGroupNames;
	private String[] expGroupTypes;
	private String[] expGroupExps;
	private transient CompiledScript[] expGroupExpScripts;
	private List<IdxRange> commonGroupIdx;
	
	public GroupOp(String[] expGroupNames, String[] expGroupTypes, String[] expGroupExps, CompiledScript[] expGroupExpScripts, List<IdxRange> commonGroupIdx){
		this.expGroupNames = expGroupNames;
		this.expGroupTypes = expGroupTypes;
		this.expGroupExps = expGroupExps;
		this.expGroupExpScripts = expGroupExpScripts;
		this.commonGroupIdx = commonGroupIdx;
	}
	
	public String[] getExpGroupNames() {
		return expGroupNames;
	}
	public void setExpGroupNames(String[] expGroupNames) {
		this.expGroupNames = expGroupNames;
	}
	public String[] getExpGroupTypes() {
		return expGroupTypes;
	}
	public void setExpGroupTypes(String[] expGroupTypes) {
		this.expGroupTypes = expGroupTypes;
	}
	public String[] getExpGroupExps() {
		return expGroupExps;
	}
	public void setExpGroupExps(String[] expGroupExps) {
		this.expGroupExps = expGroupExps;
	}
	public CompiledScript[] getExpGroupExpScripts() {
		return expGroupExpScripts;
	}
	public void setExpGroupExpScripts(CompiledScript[] expGroupExpScripts) {
		this.expGroupExpScripts = expGroupExpScripts;
	}
	public List<IdxRange> getCommonGroupIdx() {
		return commonGroupIdx;
	}
	public void setCommonGroupIdx(List<IdxRange> commonGroupIdx) {
		this.commonGroupIdx = commonGroupIdx;
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
