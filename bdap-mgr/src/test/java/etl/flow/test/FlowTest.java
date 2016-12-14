package etl.flow.test;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
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



public class FlowTest {
	public static final Logger logger = LogManager.getLogger(FlowTest.class);
	
	private FlowDeployer clouderaDeployer = new FlowDeployer("testFlow.cloudera.properties");
	private FlowDeployer localDeployer = new FlowDeployer("testFlow.local.properties");
	
	public void initData(FlowDeployer deployer, SftpInfo ftpInfo){
		SftpUtil.sftpFromLocal(ftpInfo, String.format("%sdata", getRelativeResourceFolder()), 
				String.format("/data/flow1/"));
		try {
			deployer.getFs().copyFromLocalFile(new Path(String.format("%sdata/sftpcfg/test1.sftp.map.properties", getRelativeResourceFolder())), 
					new Path("/flow1/sftpcfg/test1.sftp.map.properties"));
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
		mflist.add(flowMgr.genEnginePropertyFile(deployer.getEC()));
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
	public void testOozieJson() throws Exception{
		//deployer.installEngine(false);
		String projectName = "project1";
		String flowName="flow1";
		SftpInfo ftpInfo = new SftpInfo("dbadmin", "password", "192.85.247.104", 22);
		initData(clouderaDeployer, ftpInfo);
		clouderaDeployer.runDeploy(projectName, flowName, null, true, EngineType.oozie);
		String wfId = clouderaDeployer.runExecute(projectName, flowName);
		OozieFlowMgr ofm = new OozieFlowMgr();
		FlowInfo fi=null;
		while (true){
			try {
				fi = ofm.getFlowInfo(projectName, clouderaDeployer.getOC(), wfId);
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
		List<String> ls = HdfsUtil.listDfsFile(clouderaDeployer.getFs(), String.format("/flow1/csvmerge/%s", wfId));
		assertTrue(ls.contains("part-r-00000"));
		List<String> contents = HdfsUtil.stringsFromDfsFile(clouderaDeployer.getFs(), 
				String.format("/flow1/csvmerge/%s/%s", wfId, "part-r-00000"));
		logger.info(String.format("contents:\n%s", String.join("\n", contents)));
		String[] csv = contents.get(0).split(",");
		assertTrue(csv[8].equals(wfId));
	}
	
	@Test
	public void testLocalSparkCmd() throws Exception{
		String wfName= "flow1";
		String wfId="wfid1";
		localDeployer.getFs().delete(new Path(String.format("/%s/mergecsv/%s", wfName, wfId)), true);
		SftpInfo ftpInfo = new SftpInfo("dbadmin", "password", "192.85.247.104", 22);
		initData(localDeployer, ftpInfo);
		Flow1SparkCmd psf = new Flow1SparkCmd(wfName, wfId, null, localDeployer.getDefaultFS(), null);
		psf.setResFolder("src/test/resources/flow1/");
		psf.setMasterUrl("local[5]");
		psf.sgProcess();
		//assertion
		String fileName="singleTable";
		List<String> ls = HdfsUtil.listDfsFile(localDeployer.getFs(), String.format("/flow1/mergecsv/%s/%s", wfId, fileName));
		assertTrue(ls.contains(fileName));
		List<String> contents = HdfsUtil.stringsFromDfsFile(localDeployer.getFs(), 
				String.format("/flow1/mergecsv/%s/%s", wfId, fileName));
		logger.info(String.format("contents:\n%s", String.join("\n", contents)));
		String[] csv = contents.get(0).split(",");
		assertTrue(csv[8].equals(wfId));
	}

	@Test
	public void testSparkJson(){
		String projectName = "project1";
		String flowName="flow1";
		//ft.initData();
		clouderaDeployer.runDeploy(projectName, flowName, null, true, EngineType.spark);
		//deployer.runExecute(projectName, flowName);
	}
}
