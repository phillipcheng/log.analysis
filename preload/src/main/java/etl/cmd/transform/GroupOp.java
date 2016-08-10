package etl.cmd.transform;

import java.util.List;

import javax.script.CompiledScript;

import etl.util.IdxRange;

public class GroupOp {
	
	private String[] expGroupNames;
	private String[] expGroupTypes;
	private String[] expGroupExps;
	private CompiledScript[] expGroupExpScripts;
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
}
