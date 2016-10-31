package etl.flow;

import java.util.Objects;

public class Data {
	
	private String name;
	private String location;//hdfs location
	private String schemaName = null;//reference to schema
	private boolean instance = true; //if instance is true, the input path is location/$wfid
	
	public Data(){
	}
	
	public Data(String name, String location){
		this.name = name;
		this.location = location;
	}

	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof Data)){
			return false;
		}
		Data that = (Data)obj;
		if (!Objects.equals(that.getName(), name)){
			return false;
		}
		if (!Objects.equals(that.getLocation(), location)){
			return false;
		}
		return true;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public boolean isInstance() {
		return instance;
	}

	public void setInstance(boolean instance) {
		this.instance = instance;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
}
