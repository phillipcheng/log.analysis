package etl.flow;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

public class CallSubFlowNode extends Node{
	
	private String prjName;
	private String subFlowName;
	private Map<String, Object> propagateProperties = new HashMap<String, Object>();//preserving the insertion order
	
	public CallSubFlowNode(){
	}
	
	public CallSubFlowNode(String name){
		super(name);
	}
	
	@Override
	public boolean equals(Object obj){
		if (!super.equals(obj)){
			return false;
		}
		if (!(obj instanceof CallSubFlowNode)){
			return false;
		}
		CallSubFlowNode that = (CallSubFlowNode)obj;
		if (!that.getSubFlowName().equals(getSubFlowName())){
			return false;
		}
		return true;
	}

	public String getSubFlowName() {
		return subFlowName;
	}

	public void setSubFlowName(String subFlowName) {
		this.subFlowName = subFlowName;
	}
	
	public Object getPropagateProperty(String key){
		return propagateProperties.get(key);
	}
	@JsonAnySetter
	public void putPropagateProperty(String key, Object value){
		propagateProperties.put(key, value);
	}
	@JsonAnyGetter
	public Map<String, Object> getPropagateProperties() {
		return propagateProperties;
	}
	public String getPrjName() {
		return prjName;
	}
	public void setPrjName(String prjName) {
		this.prjName = prjName;
	}
	
}
