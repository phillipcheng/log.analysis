package etl.flow;

import java.util.ArrayList;
import java.util.List;
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
	private List<NodeLet> inlets = new ArrayList<NodeLet>();
	private List<NodeLet> outlets = new ArrayList<NodeLet>();
	
	public Node(){	
	}
	
	public Node(String name){
		this.name = name;
	}
	
	public String toString(){
		return String.format("%s,%d,%d", name);
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
		return true;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public void addInLet(NodeLet nl){
		inlets.add(nl);
	}
	public void addOutLet(NodeLet nl){
		outlets.add(nl);
	}
	
	public List<NodeLet> getInLets(){
		return inlets;
	}
	
	public List<NodeLet> getOutlets(){
		return outlets;
	}
}
