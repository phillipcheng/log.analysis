package etl.flow;

import java.util.Objects;

public class DataSchema {
	
	private String name;
	private String location; //where the logic schema file is
	
	public DataSchema(){
	}
	
	public DataSchema(String name, String location){
		this.name = name;
		this.location = location;
	}
	
	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof DataSchema)){
			return false;
		}
		DataSchema that = (DataSchema)obj;
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

}
