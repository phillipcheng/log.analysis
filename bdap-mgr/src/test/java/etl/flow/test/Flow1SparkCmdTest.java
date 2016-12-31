package etl.flow.test;

import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.JsonUtil;
import bdap.util.SftpInfo;
import bdap.util.Util;
import etl.flow.Flow;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.spark.SparkFlowMgr;
import etl.flow.test.cmd.Flow1SparkCmd;

public class Flow1SparkCmdTest {
	public static final Logger logger = LogManager.getLogger(Flow1SparkCmdTest.class);
	private FlowDeployer localDeployer = new FlowDeployer("testFlow.local.properties");
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
	
	public String getRelativeResourceFolder() {
		return "src/test/resources/flow1/";
	}
	
	public void genProperties() throws Exception{
		genProperties(localDeployer);
	}
	
	@Test
	public void testLocalSparkCmd() throws Exception{
		String prjName="project1";
		String wfName= "flow1";
		String wfId="wfid1";
		localDeployer.delete(String.format("/%s/csvmerge/%s", wfName, wfId), true);
		SftpInfo ftpInfo = new SftpInfo("dbadmin", "password", "192.85.247.104", 22);
		Flow1Test.initData(localDeployer, prjName, wfName, ftpInfo);
		
		//synchronous
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
	
}
