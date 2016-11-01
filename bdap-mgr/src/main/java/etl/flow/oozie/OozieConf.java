package etl.flow.oozie;

import org.apache.commons.configuration.PropertiesConfiguration;

import bdap.util.PropertiesUtil;
import etl.flow.mgr.FlowServerConf;

//this is needed to generate job.properties and deployment
public class OozieConf implements FlowServerConf {
	
	public static final String key_oozie_server_ip="oozie.server.ip";
	public static final String key_oozie_server_port="oozie.server.port";
	public static final String key_jobTracker="jobTracker";
	public static final String key_nameNode="nameNode";
	public static final String key_queueName="queueName";
	public static final String key_oozieLibPath="oozie.libpath";
	public static final String key_oozieWfAppPath="oozie.wf.application.path";
	public static final String key_oozieCoordinateAppPath="oozie.coord.application.path";
	public static final String key_user_name="user.name";
	
	public static final String key_oozie_action="action";
	public static final String value_action_start="start";
	
	private String oozieServerIp;
	private int oozieServerPort;
	private String nameNode;
	private String jobTracker;
	private String queueName = "default";
	private String oozieLibPath;
	private String userName;
	
	public OozieConf(String confFile){
		PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(confFile);
		oozieServerIp = pc.getString(key_oozie_server_ip);
		oozieServerPort = pc.getInt(key_oozie_server_port);
		nameNode = pc.getString(key_nameNode);
		jobTracker =pc.getString(key_jobTracker);
		userName = pc.getString(key_user_name);
	}
	
	public OozieConf(String oozieServerIp, int oozieServerPort, String nameNode, String jobTracker, String oozieLibPath, String userName){
		this.oozieServerIp = oozieServerIp;
		this.oozieServerPort = oozieServerPort;
		this.nameNode = nameNode;
		this.jobTracker = jobTracker;
		this.oozieLibPath = oozieLibPath;
		this.userName = userName;
	}
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
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getOozieServerIp() {
		return oozieServerIp;
	}
	public void setOozieServerIp(String oozieServerIp) {
		this.oozieServerIp = oozieServerIp;
	}
	public int getOozieServerPort() {
		return oozieServerPort;
	}
	public void setOozieServerPort(int oozieServerPort) {
		this.oozieServerPort = oozieServerPort;
	}
	
}
