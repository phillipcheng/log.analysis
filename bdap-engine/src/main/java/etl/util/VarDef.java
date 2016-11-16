package etl.util;

public class VarDef {
	
	private String name;
	private VarType type;
	
	public VarDef(String name, VarType type){
		this.name = name;
		this.type = type;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public VarType getType() {
		return type;
	}
	public void setType(VarType type) {
		this.type = type;
	}
}
