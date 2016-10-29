package etl.flow.test.oozie;

import java.io.File;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.EngineConf;
import bdap.util.JsonUtil;
import bdap.util.SftpUtil;
import etl.flow.Flow;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.test.TestFlow;

public class TestOozieFlow extends TestFlow{
	public static final Logger logger = LogManager.getLogger(TestOozieFlow.class);
	private String hadoopId = "192.85.247.104";
	public OozieConf getOC(){
		OozieConf oc = new OozieConf();
		oc.setOozieServerIp(hadoopId);
		oc.setOozieServerPort(11000);
		oc.setNameNode(String.format("hdfs://%s:19000", hadoopId));
		oc.setJobTracker(String.format("%s:8032", hadoopId));
		oc.setQueueName("default");
		oc.setOozieLibPath("${nameNode}/user/${user.name}/share/lib/preload/lib/");
		oc.setUserName("dbadmin");
		return oc;
	}
	
	public EngineConf getEC(){
		EngineConf ec = new EngineConf();
		ec.addProperty(EngineConf.cfgkey_defaultFs, String.format("hdfs://%s:19000", hadoopId));
		ec.addProperty(EngineConf.cfgkey_kafka_bootstrap_servers, "192.85.247.104:9092\\,192.85.247.105:9092\\,192.85.247.106:9092");
		ec.addProperty(EngineConf.cfgkey_kafka_log_topic, "log-analysis-topic");
		ec.addProperty(EngineConf.cfgkey_kafka_enabled, "true");
		
		ec.addProperty(EngineConf.cfgkey_db_type, "vertica");
		ec.addProperty(EngineConf.cfgkey_db_driver, "com.vertica.jdbc.Driver");
		ec.addProperty(EngineConf.cfgkey_db_url, "jdbc:vertica://192.85.247.104:5433/cmslab");
		ec.addProperty(EngineConf.cfgkey_db_user, "dbadmin");
		ec.addProperty(EngineConf.cfgkey_db_password, "password");
		
		ec.addProperty(EngineConf.cfgkey_hdfs_webhdfs_root, "http://192.85.247.104:50070/webhdfs/v1");
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
	
	@Test
	public void initData(){
		//setup data
		String sftpUser="dbadmin";
		String sftpPasswd="password";
		SftpUtil.sftpFromLocal(hadoopId, 22, sftpUser, sftpPasswd, String.format("%sdata", super.getResourceFolder()), 
				String.format("/data/flow1/"));
		try {
			super.getFs().copyFromLocalFile(new Path(String.format("%sdata/sftpcfg/test1.sftp.map.properties", super.getResourceFolder())), 
					new Path("/flow1/sftpcfg/test1.sftp.map.properties"));
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public void flow1Fun(){
		String projectName = "project1";
		initData();
		//start flow
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
