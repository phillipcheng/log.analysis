package etl.flow.oozie;

import etl.flow.mgr.FlowServerConf;

//this is needed to generate job.properties and deployment
public class OozieConf implements FlowServerConf {
	
	public static final String key_jobTracker="jobTracker";
	public static final String key_nameNode="nameNode";
	public static final String key_queueName="queueName";
	public static final String key_oozieLibPath="oozie.libpath";
	public static final String key_oozieWfAppPath="oozie.wf.application.path";
	public static final String key_oozieCoordinateAppPath="oozie.coord.application.path";
	
	private String jobTracker;
	private String nameNode;
	private String queueName = "default";
	private String oozieLibPath;
	
	public String getOozieLibPath() {
		return oozieLibPath;
	}
	public void setOozieLibPath(String oozieLibPath) {
		this.oozieLibPath = oozieLibPath;
	}
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
	public String getQueueName() {
		return queueName;
	}
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	
}
