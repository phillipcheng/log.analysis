package etl.flow.test;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import bdap.util.SftpInfo;
import bdap.util.SftpUtil;
import etl.engine.types.DBType;
import etl.engine.types.ProcessMode;
import etl.flow.deploy.DefaultDeployMethod;
import etl.flow.deploy.EngineType;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.spark.SparkFlowMgr;
import etl.flow.test.cmd.Flow1SparkCmd;
import etl.util.SchemaUtils;

public class FlowTest {
	public static final Logger logger = LogManager.getLogger(FlowTest.class);
	
	private static FlowDeployer apacheDeployer = new FlowDeployer("testFlow.apache.properties");
	private static FlowDeployer localDeployer = new FlowDeployer("testFlow.local.properties");
	private static SftpInfo ftpInfo = new SftpInfo(apacheDeployer.getPc().getString("ssh.server.user", "dbadmin"), 
			apacheDeployer.getPc().getString("ssh.server.passwd", "password"),
			apacheDeployer.getPc().getString("ssh.server.ip", "192.85.247.104"), 
			apacheDeployer.getPc().getInt("ssh.server.port", 22));
	
	private String[] jars = new String[]{"target/bdap.mgr-r0.6.0-tests.jar"};
	
	public void genSql() throws Exception{
		String outputSql = "src/test/resources/sql/flow1.sql";
		String schemaFile = "src/test/resources/schema/flow1.schema";
		String dbSchema = "project1";
		SchemaUtils.genCreateSqls(schemaFile, outputSql, dbSchema, DBType.VERTICA);
	}
	
	public static void initData(FlowDeployer deployer, String prjName, String flowName){
		String hdfsFolder = deployer.getProjectHdfsDir(prjName);
		SftpUtil.sftpFromLocal(ftpInfo, String.format("%sdata", getRelativeResourceFolder()), 
				String.format(deployer.getPc().getString("ssh.server.data.dir", "") + "/data/flow1/"));
		deployer.delete(String.format("%s%s", hdfsFolder, flowName), true);
		deployer.copyFromLocalFile(false, true, String.format("%sdata/sftpcfg/test1.sftp.map.properties", getRelativeResourceFolder()), 
				"/flow1/sftpcfg/test1.sftp.map.properties");
		deployer.copyFromLocalFile(false, true, String.format("%sschema/flow1.schema", getRelativeResourceFolder()), 
				"/flow1/schema/flow1.schema");
	}
	
	public static String getRelativeResourceFolder() {
		return "src/test/resources/";
	}
	
	public void assertResults(List<String> contents, String wfId){
		assertTrue(contents.size()==16);
		logger.info(String.format("contents:\n%s", String.join("\n", contents)));
		String[] csv = contents.get(0).split(",");
		assertTrue(csv[6].equals(wfId));
	}
	
	public void testFlow1(EngineType et, FlowMgr flowMgr) throws Exception{
		apacheDeployer.installEngine(false);
		String prjName = "project1";
		String flowName="flow1";
		initData(apacheDeployer, prjName, flowName);
		apacheDeployer.runDeploy(prjName, flowName, jars, null, et);
		String wfId = apacheDeployer.runExecute(prjName, flowName, et);
		FlowInfo fi=null;
		while (true){
			try {
				FlowServerConf serverConf;
				if (EngineType.oozie.equals(et))
					serverConf = apacheDeployer.getOozieServerConf();
				else
					serverConf = apacheDeployer.getSparkServerConf();
				fi = flowMgr.getFlowInfo(prjName, serverConf, wfId);
				logger.info(String.format("flow info for instance:%s:%s", wfId, fi));
				Thread.sleep(5000);
				if (!"RUNNING".equals(fi.getStatus())){
					break;
				}
			}catch(Exception e){
				logger.error("", e);
			}
		}
		//assertion after finished
		FileSystem fs = ((DefaultDeployMethod)apacheDeployer.getDeployMethod()).getFs();
		List<String> contents = HdfsUtil.stringsFromDfsFolder(fs, String.format("/flow1/csvmerge/%s", wfId));
		assertResults(contents, wfId);
	}
	
	//179
	@Test
	public void testFlow1OozieFromJson() throws Exception{
		testFlow1(EngineType.oozie, new OozieFlowMgr());
	}
	//70
	@Test
	public void testFlow1SparkFromJson() throws Exception{
		testFlow1(EngineType.spark, new SparkFlowMgr());
	}
	
	public void testFlow1LocalSparkCmd() throws Exception{
		String wfId = "wfId";
		String defaultFs=localDeployer.getDefaultFS();
		String resFolder = "C:\\mydoc\\myprojects\\bdap\\bdap-mgr\\src\\test\\resources\\flow1\\";
		String prjName = "project1";
		String flowName="flow1";
		String output = String.format("%s/%s/csvmerge/%s", defaultFs, flowName, wfId);
		FileSystem fs = HdfsUtil.getHadoopFs(defaultFs);
		fs.delete(new Path(output), true);
		initData(localDeployer, prjName, flowName);
		Flow1SparkCmd cmd = new Flow1SparkCmd(flowName, wfId, null, defaultFs, null, ProcessMode.Single);
		cmd.setMasterUrl("local[5]");
		cmd.setResFolder(resFolder);
		List<String> ret = cmd.sgProcess();
		assertResults(ret, wfId);
	}
	
	public void testFlow2(EngineType et) throws Exception{
		String prjName = "project1";
		String flow1="flow1";
		String flow2="flow2";
		initData(apacheDeployer, prjName, flow1);
		apacheDeployer.runDeploy(prjName, flow1, jars, null, et);
		apacheDeployer.runDeploy(prjName, flow2, jars, null, EngineType.oozie);
		String wfId = apacheDeployer.runExecute(prjName, flow2, EngineType.oozie);
		logger.info(String.format("wfid:%s", wfId));
	}
	
	
	
	public void testFlow2Oozie() throws Exception{
		testFlow2(EngineType.oozie);
	}
	
	public void testFlow2Spark() throws Exception{
		testFlow2(EngineType.spark);
	}
}
