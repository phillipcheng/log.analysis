package etl.flow.test;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.JsonUtil;
import bdap.util.SftpInfo;
import bdap.util.SftpUtil;
import bdap.util.Util;
import etl.flow.Flow;
import etl.flow.deploy.EngineType;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.spark.SparkFlowMgr;
import etl.flow.test.cmd.Flow1SparkCmd;

public class FlowTest {
	public static final Logger logger = LogManager.getLogger(FlowTest.class);
	
	private FlowDeployer apacheDeployer = new FlowDeployer("testFlow.apache.properties");
	private FlowDeployer localDeployer = new FlowDeployer("testFlow.local.properties");
	
	public void initData(FlowDeployer deployer, SftpInfo ftpInfo){
		SftpUtil.sftpFromLocal(ftpInfo, String.format("%sdata", getRelativeResourceFolder()), 
				String.format("/data/flow1/"));
		try {
			deployer.copyFromLocalFile(String.format("%sdata/sftpcfg/test1.sftp.map.properties", getRelativeResourceFolder()), 
					"/flow1/sftpcfg/test1.sftp.map.properties");
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public String getRelativeResourceFolder() {
		return "src/test/resources/flow1/";
	}
	
	//both action properties and engine.properties
	public void genProperties(FlowDeployer deployer) throws Exception{
		Flow flow1 = (Flow) JsonUtil.fromLocalJsonFile(getRelativeResourceFolder()+"flow1.json", Flow.class);
		SparkFlowMgr flowMgr = new SparkFlowMgr();
		List<InMemFile> mflist = flowMgr.genProperties(flow1);
		mflist.add(flowMgr.genEnginePropertyFile(deployer.getEngineConfig()));
		for (InMemFile mf:mflist){
			Files.write(Paths.get(String.format("%s%s", getRelativeResourceFolder(), mf.getFileName())), mf.getContent());
		}
	}

	public void genOozieXml(){
		OozieFlowMgr ofm = new OozieFlowMgr();
		String flowFile = "flow1/flow1.json";
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(flowFile, Flow.class);
		String flowXml = ofm.genWfXmlFile(flow);
		Util.writeFile(getRelativeResourceFolder() + "flow1_workflow.xml", flowXml);
	}
	
	public void genProperties() throws Exception{
		genProperties(localDeployer);
	}
	
	@Test
	public void testApacheOozieJson() throws Exception{
		apacheDeployer.installEngine(false);
		String projectName = "project1";
		String flowName="flow1_oozie";
		SftpInfo ftpInfo = new SftpInfo("dbadmin", "password", "192.85.247.104", 22);
		initData(apacheDeployer, ftpInfo);
		apacheDeployer.runDeploy(projectName, flowName, null, true, EngineType.oozie);
		String wfId = apacheDeployer.runExecute(projectName, flowName, EngineType.oozie);
		OozieFlowMgr ofm = new OozieFlowMgr();
		FlowInfo fi=null;
		while (true){
			try {
				fi = ofm.getFlowInfo(projectName, apacheDeployer.getOozieServerConf(), wfId);
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
		List<String> ls = apacheDeployer.listFiles(String.format("/flow1/csvmerge/%s", wfId));
		String fileName="singleTable-r-00000";
		assertTrue(ls.contains(fileName));
		List<String> contents = apacheDeployer.readFile(String.format("/flow1/csvmerge/%s/%s", wfId, fileName));
		logger.info(String.format("contents:\n%s", String.join("\n", contents)));
		String[] csv = contents.get(0).split(",");
		assertTrue(csv[8].equals(wfId));
	}
	
	@Test
	public void testLocalSparkCmd() throws Exception{
		String wfName= "flow1";
		String wfId="wfid1";
		localDeployer.delete(String.format("/%s/csvmerge/%s", wfName, wfId), true);
		SftpInfo ftpInfo = new SftpInfo("dbadmin", "password", "192.85.247.104", 22);
		initData(localDeployer, ftpInfo);
		
		Flow1SparkCmd psf = new Flow1SparkCmd(wfName, wfId, null, localDeployer.getDefaultFS(), null);
		psf.setResFolder("src/test/resources/flow1_oozie/");
		psf.setMasterUrl("local[5]");
		psf.sgProcess();
		
		//assertion
		String fileName="singleTable";
		List<String> ls = localDeployer.listFiles(String.format("/flow1/csvmerge/%s/%s", wfId, fileName));
		assertTrue(ls.contains(fileName));
		List<String> contents = localDeployer.readFile(String.format("/flow1/csvmerge/%s/%s", wfId, fileName));
		logger.info(String.format("contents:\n%s", String.join("\n", contents)));
		String[] csv = contents.get(0).split(",");
		assertTrue(csv[8].equals(wfId));
	}

	@Test
	public void testApacheSparkJson() throws Exception{
		String projectName = "project1";
		String flowName="flow1_spark";
		apacheDeployer.installEngine(false);
		
		SftpInfo ftpInfo = new SftpInfo("dbadmin", "password", "192.85.247.104", 22);
		initData(apacheDeployer, ftpInfo);
		
		apacheDeployer.runDeploy(projectName, flowName, null, true, EngineType.spark);
		String wfId = apacheDeployer.runExecute(projectName, flowName, EngineType.spark);
		
		OozieFlowMgr ofm = new OozieFlowMgr();
		FlowInfo fi=null;
		while (true){
			try {
				fi = ofm.getFlowInfo(projectName, apacheDeployer.getOozieServerConf(), wfId);
				logger.info(String.format("flow info for instance:%s:%s", wfId, fi));
				Thread.sleep(5000);
				if (!fi.getStatus().equals("RUNNING")){
					break;
				}
			}catch(Exception e){
				logger.error("", e);
			}
		}
		
		//assertion
		String fileName="singleTable";
		List<String> ls = apacheDeployer.listFiles(String.format("/flow1/csvmerge/%s/%s", wfId, fileName));
		assertTrue(ls.contains(fileName));
		List<String> contents = apacheDeployer.readFile(String.format("/flow1/csvmerge/%s/%s", wfId, fileName));
		logger.info(String.format("contents:\n%s", String.join("\n", contents)));
		String[] csv = contents.get(0).split(",");
		assertTrue(csv[8].equals(wfId));
	}
}
