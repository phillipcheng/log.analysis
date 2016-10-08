package etl.flow;

import java.util.LinkedHashMap;
import java.util.Map;

public class ActionNode extends Node{
	
	public static final String key_exe_type="exe.type";//
	public static final String key_cmd_class="cmd.class";
	public static final String key_input_format="input.format";
	
	private Map<String, String> properties = new LinkedHashMap<String, String>();//preserving the insertion order
	
	private ExeType exeType;
	
	public ActionNode(String name, ExeType exeType, int inletNum, int outletNum){
		super(name, inletNum, outletNum);
		this.exeType = exeType;
	}

	public String getProperty(String key){
		return properties.get(key);
	}
	
	public void put(String key, String value){
		properties.put(key, value);
	}
	
	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public ExeType getExeType() {
		return exeType;
	}

	public void setExeType(ExeType exeType) {
		this.exeType = exeType;
	}
}
