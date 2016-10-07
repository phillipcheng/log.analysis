package etl.flow;

import java.util.HashMap;
import java.util.Map;

public class ActionNode extends Node{
	
	public static final String key_exe_type="exe.type";//
	public static final String key_cmd_class="cmd.class";
	
	private Map<String, String> properties = new HashMap<String, String>();
	
	private ExeType exeType;
	
	public ActionNode(String name, ExeType exeType, int inletNum, int outletNum){
		super(name, inletNum, outletNum);
		this.exeType = exeType;
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
