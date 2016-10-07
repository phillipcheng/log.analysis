package etl.flow;

public class StartNode extends Node{

	public static final String start_node_name="start";

	private int duration=30;//in seconds
	
	public StartNode(int duration, int inlet, int outlet) {
		super(start_node_name, inlet, outlet);
		this.duration = duration;
	}
	
	public int getDuration() {
		return duration;
	}


	public void setDuration(int duration) {
		this.duration = duration;
	}

}
