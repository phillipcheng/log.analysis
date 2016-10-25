package etl.flow.oozie.test;

import java.io.File;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.EngineConf;
import bdap.util.JsonUtil;
import etl.flow.Flow;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;

public class TestOozieFlow {
	public static final Logger logger = LogManager.getLogger(TestOozieFlow.class);
	private String hadoopId = "192.85.247.104";
	public OozieConf getOC(){
		OozieConf oc = new OozieConf();
		oc.setOozieServerIp(hadoopId);
		oc.setOozieServerPort(11000);
		oc.setNameNode(String.format("hdfs://%s:19000", hadoopId));
		oc.setJobTracker(String.format("%s:8032", hadoopId));
		oc.setQueueName("default");
		oc.setOozieLibPath("${nameNode}/user/${user.name}/preload/lib/");
		oc.setUserName("dbadmin");
		return oc;
	}
	
	public EngineConf getEC(){
		EngineConf ec = new EngineConf();
		ec.addProperty(EngineConf.cfgkey_defaultFs, String.format("hdfs://%s:19000", hadoopId));
		ec.addProperty(EngineConf.cfgkey_db_type, EngineConf.value_db_none);
		return ec;
	}
	
	@Test
	public void genFlow1(){
		OozieFlowMgr ofm = new OozieFlowMgr();
		String flowFile = getResourceSubFolder()+"flow1.json";
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(flowFile, Flow.class);
		String flowXml = ofm.genWfXmlFile(flow);
		logger.error("\n" + flowXml);
	}
	public void flow1Fun(){
		String projectName = "project1";
		OozieFlowMgr ofm = new OozieFlowMgr();
		String flowFile = getResourceSubFolder()+"flow1.json";
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(flowFile, Flow.class);
		ofm.execute(projectName, flow, getOC(), getEC(), null, null);
	}
	
	@Test
	public void testFlow1() throws Exception{
		if (hadoopId.equals("127.0.0.1")){
			flow1Fun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					flow1Fun();
					return null;
				}
			});
		}
	}

	public String getResourceSubFolder() {
		return "flow" + File.separator;
	}
}
