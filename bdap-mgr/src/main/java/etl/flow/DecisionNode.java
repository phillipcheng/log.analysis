package etl.flow;

public class DecisionNode extends Node{
	public static final String Decision_node_name="decision";
	private String case_condition;
	private String case_default;
	private String flow_default;
	public DecisionNode(){
		super(Decision_node_name);
	}
	
	public DecisionNode(String name){
		super(name);
	}

	public String getCase_condition() {
		return case_condition;
	}

	public void setCase_condition(String case_condition) {
		this.case_condition = case_condition;
	}

	public String getCase_default() {
		return case_default;
	}

	public void setCase_default(String case_default) {
		this.case_default = case_default;
	}

	public String getFlow_default() {
		return flow_default;
	}

	public void setFlow_default(String flow_default) {
		this.flow_default = flow_default;
	}
	
}
