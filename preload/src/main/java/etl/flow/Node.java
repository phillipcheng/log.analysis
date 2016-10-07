package etl.flow;

public class Node{
	
	private String name;
	private int inletNum = 0;
	private int outletNum = 0;
	
	public Node(String name, int inletNum, int outletNum){
		this.name = name;
		this.inletNum = inletNum;
		this.outletNum = outletNum;
	}
	
	@Override
	public boolean equals(Object obj){
		if (obj instanceof Node){
			Node that = (Node) obj;
			if (name.equals(that.getName())){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public int getInletNum() {
		return inletNum;
	}

	public void setInletNum(int inletNum) {
		this.inletNum = inletNum;
	}

	public int getOutletNum() {
		return outletNum;
	}

	public void setOutletNum(int outletNum) {
		this.outletNum = outletNum;
	}
}
