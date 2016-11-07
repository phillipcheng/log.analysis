package etl.flow.test.oozie;

import java.io.File;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.JsonUtil;
import bdap.util.SftpUtil;
import etl.flow.Flow;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.test.TestFlow;

public class TestOozieFlow extends TestFlow{
	public static final Logger logger = LogManager.getLogger(TestOozieFlow.class);
	
	@Test
	public void genFlow1(){
		OozieFlowMgr ofm = new OozieFlowMgr();
		String flowFile = getResourceFolder()+"flow1.json";
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(flowFile, Flow.class);
		String flowXml = ofm.genWfXmlFile(flow);
		logger.error("\n" + flowXml);
	}
	
	@Test
	public void initData(){
		//setup data
		String sftpUser="dbadmin";
		String sftpPasswd="password";
		SftpUtil.sftpFromLocal(getOC().getOozieServerIp(), 22, sftpUser, sftpPasswd, String.format("%sdata", getResourceFolder()), 
				String.format("/data/flow1/"));
		try {
			super.getFs().copyFromLocalFile(new Path(String.format("%sdata/sftpcfg/test1.sftp.map.properties", getResourceFolder())), 
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
		String flowFile = getResourceFolder()+"flow1.json";
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(flowFile, Flow.class);
		ofm.execute(projectName, flow, null, getOC(), getEC(), null, null);
	}
	
	@Test
	public void testFlow1() throws Exception{
		if (getOC().getNameNode().equals("127.0.0.1")){
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

	public String getResourceFolder() {
		return super.getLocalFolder() + File.separator + "flow" + File.separator;
	}
}
