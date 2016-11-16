package etl.flow;

public class StartNode extends Node{

	public static final String start_node_name="start";

	private int duration=30;//in seconds
	
	public StartNode(){	
	}
	
	public StartNode(int duration) {
		super(start_node_name);
		this.duration = duration;
	}
	
	public int getDuration() {
		return duration;
	}


	public void setDuration(int duration) {
		this.duration = duration;
	}

}
