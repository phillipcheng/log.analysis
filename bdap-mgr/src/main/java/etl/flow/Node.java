package etl.flow;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.PROPERTY, property = "@class")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ActionNode.class, name = "action"),
    @JsonSubTypes.Type(value = StartNode.class, name = "start"),
    @JsonSubTypes.Type(value = EndNode.class, name = "end"),
    @JsonSubTypes.Type(value = Flow.class, name = "flow")
})
public class Node{
	
	private String name;
	private int inletNum = 0;
	private int outletNum = 0;
	
	public Node(){	
	}
	
	public Node(String name, int inletNum, int outletNum){
		this.name = name;
		this.inletNum = inletNum;
		this.outletNum = outletNum;
	}
	
	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof Node)){
			return false;
		}
		Node that = (Node) obj;
		if (!Objects.equals(that.getName(), name)){
			return false;
		}
		if (that.getInletNum()!=inletNum){
			return false;
		}
		if (that.getOutletNum()!=outletNum){
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
