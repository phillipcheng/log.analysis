package etl.flow;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class ActionNode extends Node{
	
	public static final String key_exe_type="exe.type";//
	public static final String key_cmd_class="cmd.class";
	public static final String key_input_format="input.format";
	
	public static List<String> sysProperties = null;
	
	private Map<String, String> properties = new LinkedHashMap<String, String>();//preserving the insertion order
	
	@JsonIgnore
	private ExeType exeType;
	
	public ActionNode(){
	}
	
	public ActionNode(String name, ExeType exeType, int inletNum, int outletNum){
		super(name, inletNum, outletNum);
		this.exeType = exeType;
	}
	
	@Override
	public boolean equals(Object obj){
		if (!super.equals(obj)){
			return false;
		}
		if (!(obj instanceof ActionNode)){
			return false;
		}
		ActionNode that = (ActionNode)obj;
		if (!that.getProperties().equals(getProperties())){
			return false;
		}
		return true;
	}
	
	public static List<String> getSysProperties(){
		if (sysProperties==null){
			sysProperties = Arrays.asList(new String[]{key_exe_type, key_cmd_class, key_input_format});
		}
		return sysProperties;
	}
	
	@JsonIgnore
	public Map<String, String> getUserProperties(){
		Map<String, String> userProperties = new LinkedHashMap<String, String>();
		for (String key: properties.keySet()){
			if (!getSysProperties().contains(key)){
				userProperties.put(key, properties.get(key));
			}
		}
		return userProperties;
	}

	public String getProperty(String key){
		return properties.get(key);
	}
	@JsonAnySetter
	public void putProperty(String key, String value){
		properties.put(key, value);
	}
	@JsonAnyGetter
	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	@JsonIgnore
	public ExeType getExeType() {
		return exeType;
	}

	@JsonIgnore
	public void setExeType(ExeType exeType) {
		this.exeType = exeType;
	}
}
