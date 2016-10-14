package etl.flow.oozie;

import etl.flow.mgr.FlowServerConf;

//this is needed to generate job.properties and deployment
public class OozieConf implements FlowServerConf {
	
	private String jobTracker;
	private String nameNode;
	
	
	public String getJobTracker() {
		return jobTracker;
	}
	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}
	public String getNameNode() {
		return nameNode;
	}
	public void setNameNode(String nameNode) {
		this.nameNode = nameNode;
	}
	
	
	
}
