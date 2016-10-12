package etl.flow;

public class EndNode extends Node{

	public static final String end_node_name="end";

	public EndNode(){
	}
	
	public EndNode(int inlet, int outlet) {
		super(end_node_name, inlet, outlet);
	}

}
