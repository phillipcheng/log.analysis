package etl.flow.test.spark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.flow.deploy.EngineType;
import etl.flow.deploy.FlowDeployer;
import etl.flow.test.FlowTest;
import etl.flow.test.oozie.TestOozieFlow;

public class TestSparkFlow {
	public static final Logger logger = LogManager.getLogger(TestOozieFlow.class);
	
	private FlowDeployer deployer = new FlowDeployer("testFlow.cloudera.properties");
	private FlowTest ft = new FlowTest();
	
	@Test
	public void testJsonFlow1(){
		String projectName = "project1";
		String flowName="flow1";
		//ft.initData();
		deployer.runDeploy(projectName, flowName, null, true, EngineType.spark);
		//deployer.runExecute(projectName, flowName);
	}
}
