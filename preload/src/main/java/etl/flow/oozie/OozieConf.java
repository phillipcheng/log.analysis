package etl.flow.oozie;

//this is needed to generate job.properties and deployment
public class OozieConf {
	
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
