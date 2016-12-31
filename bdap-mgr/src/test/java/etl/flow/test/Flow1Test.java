package etl.flow.test;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import bdap.util.SftpInfo;
import bdap.util.SftpUtil;
import etl.flow.deploy.DefaultDeployMethod;
import etl.flow.deploy.EngineType;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.oozie.OozieFlowMgr;

public class Flow1Test {
	public static final Logger logger = LogManager.getLogger(Flow1Test.class);
	
	private FlowDeployer apacheDeployer = new FlowDeployer("testFlow.apache.properties");
	
	private String[] jars = new String[]{"target/bdap.mgr-VVERSIONN-tests.jar"};
	
	public static void initData(FlowDeployer deployer, String prjName, String flowName, SftpInfo ftpInfo){
		String hdfsFolder = deployer.getProjectHdfsDir(prjName);
		SftpUtil.sftpFromLocal(ftpInfo, String.format("%sdata", getRelativeResourceFolder()), 
				String.format("/data/flow1/"));
		deployer.delete(String.format("%s%s", hdfsFolder, flowName), true);
		deployer.copyFromLocalFile(false, true, String.format("%sdata/sftpcfg/test1.sftp.map.properties", getRelativeResourceFolder()), 
				"/flow1/sftpcfg/test1.sftp.map.properties");
		deployer.copyFromLocalFile(false, true, String.format("%sschema/flow1.schema", getRelativeResourceFolder()), 
				"/flow1/schema/flow1.schema");
		
	}
	
	public static String getRelativeResourceFolder() {
		return "src/test/resources/flow1/";
	}
	
	public void testFlow1(boolean useJson, EngineType et) throws Exception{
		apacheDeployer.installEngine(false);
		String prjName = "project1";
		String flowName="flow1";
		SftpInfo ftpInfo = new SftpInfo("dbadmin", "password", "192.85.247.104", 22);
		initData(apacheDeployer, prjName, flowName, ftpInfo);
		apacheDeployer.runDeploy(prjName, flowName, jars, useJson, et);
		String wfId = apacheDeployer.runExecute(prjName, flowName, et);
		OozieFlowMgr ofm = new OozieFlowMgr();
		FlowInfo fi=null;
		while (true){
			try {
				fi = ofm.getFlowInfo(prjName, apacheDeployer.getOozieServerConf(), wfId);
				logger.info(String.format("flow info for instance:%s:%s", wfId, fi));
				Thread.sleep(5000);
				if (!fi.getStatus().equals("RUNNING")){
					break;
				}
			}catch(Exception e){
				logger.error("", e);
			}
		}
		//assertion after finished
		FileSystem fs = ((DefaultDeployMethod)apacheDeployer.getDeployMethod()).getFs();
		List<String> contents = HdfsUtil.stringsFromDfsFolder(fs, String.format("/flow1/csvmerge/%s", wfId));
		assertTrue(contents.size()==16);
		logger.info(String.format("contents:\n%s", String.join("\n", contents)));
		String[] csv = contents.get(0).split(",");
		assertTrue(csv[6].equals(wfId));
	}
	
	@Test
	public void testOozieFromXml() throws Exception{
		testFlow1(false, EngineType.oozie);
	}
	
	@Test
	public void testOozieFromJson() throws Exception{
		testFlow1(true, EngineType.oozie);
	}
	
	@Test
	public void testSparkFromJson() throws Exception{
		testFlow1(true, EngineType.spark);
	}
}
