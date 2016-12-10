package etl.flow.test.oozie;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.JsonUtil;
import bdap.util.Util;
import etl.flow.Flow;
import etl.flow.deploy.EngineType;
import etl.flow.deploy.FlowDeployer;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.test.FlowTest;

public class TestOozieFlow {
	public static final Logger logger = LogManager.getLogger(TestOozieFlow.class);
	
	private FlowDeployer deployer = new FlowDeployer("testFlow.cloudera.properties");
	private FlowTest ft = new FlowTest();
	
	
	@Test
	public void genFlow1Xml(){
		OozieFlowMgr ofm = new OozieFlowMgr();
		String flowFile = "flow1/flow1.json";
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(flowFile, Flow.class);
		String flowXml = ofm.genWfXmlFile(flow);
		Util.writeFile(ft.getRelativeResourceFolder() + "flow1_workflow.xml", flowXml);
	}
	
	@Test
	public void testJsonFlow1() throws Exception{
		deployer.installEngine(false);
		String projectName = "project1";
		String flowName="flow1";
		ft.initData();
		deployer.runDeploy(projectName, flowName, null, true, EngineType.oozie);
		deployer.runExecute(projectName, flowName);
	}
	
	@Test
	public void testXmlFlow1(){
		String projectName = "project1";
		String flowName="flow1";
		ft.initData();
		deployer.runDeploy(projectName, flowName, null, false, EngineType.oozie);
		deployer.runExecute(projectName, flowName);
	}

}
