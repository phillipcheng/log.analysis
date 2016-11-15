package etl.flow;

public class NodeLet {

	private String name;
	private String dataName=null;
	
	public NodeLet(){
	}
	
	public NodeLet(String name){
		this.name = name;
	}
	
	public NodeLet(String name, String dataName){
		this.name = name;
		this.dataName = dataName;
	}

	public String getName() {
		return name;
	}

	public String getDataName() {
		return dataName;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setDataName(String dataName) {
		this.dataName = dataName;
	}
}
