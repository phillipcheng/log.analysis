package etl.flow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import bdap.util.PropertiesUtil;

public class ActionNode extends Node{
	
	public static final String key_exe_type="exe.type";//
	public static final String key_cmd_class="cmd.class";
	public static final String key_input_format="input.format";
	
	public static List<String> sysProperties = null;
	
	private Map<String, String> properties = new LinkedHashMap<String, String>();//preserving the insertion order
	private List<String> addArgs = new ArrayList<String>();//additional list of arguments
	
	public ActionNode(){
	}
	
	public ActionNode(String name){
		super(name);
	}
	
	public ActionNode(String name, ExeType exeType){
		super(name);
		properties.put(key_exe_type, exeType.toString());
	}
	
	public ActionNode(String name, ExeType exeType, InputFormatType ift){
		this(name, exeType);
		properties.put(key_input_format, ift.toString());
	}
	
	//for writing test cases to construct action node from existing properties file
	public ActionNode(String name, ExeType exeType, String propertiesFile){
		this(name, exeType);
		LinkedHashMap<String, String> map = PropertiesUtil.getPropertiesExactMap(propertiesFile);
		for (String key: map.keySet()){
			properties.put(key, map.get(key));
		}
	}
	
	//for writing test cases to construct action node from existing properties file
	public ActionNode(String name, ExeType exeType, InputFormatType ift, String propertiesFile){
		this(name, exeType, propertiesFile);
		properties.put(key_input_format, ift.toString());
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
	public LinkedHashMap<String, String> getUserProperties(){
		LinkedHashMap<String, String> userProperties = new LinkedHashMap<String, String>();
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

	public List<String> getAddArgs() {
		return addArgs;
	}

	public void setAddArgs(List<String> addArgs) {
		this.addArgs = addArgs;
	}
}
