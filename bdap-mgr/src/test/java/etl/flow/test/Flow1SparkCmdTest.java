package etl.flow.test;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.SftpInfo;
import etl.flow.deploy.FlowDeployer;
import etl.flow.test.cmd.Flow1SparkCmd;

public class Flow1SparkCmdTest {
	public static final Logger logger = LogManager.getLogger(Flow1SparkCmdTest.class);
	private FlowDeployer localDeployer = new FlowDeployer("testFlow.local.properties");
	
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
