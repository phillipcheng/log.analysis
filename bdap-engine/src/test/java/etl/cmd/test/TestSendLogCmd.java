package etl.cmd.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import etl.cmd.KafkaMsgDecodeCmd;
import etl.cmd.KafkaMsgGenCmd;
import etl.cmd.SendLogCmd;
import etl.engine.ETLCmd;
import etl.engine.ETLCmdMain;

public class TestSendLogCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestSendLogCmd.class);

	@Override
	public String getResourceSubFolder() {
		return "sendmsg/";
	}
	
	@Before
	public void beforeMethod() {
		org.junit.Assume.assumeTrue(super.isTestKafka());
	}
	
	@Test
	public void testSendLog() throws Exception {
		String dfsFolder = "/test/sendlog/";
		String wfid = "wf1";
		
		getFs().delete(new Path(dfsFolder), true);
		getFs().mkdirs(new Path(dfsFolder));
		
		// run cmd
		String[] infoArray = new String[]{"cmd1", "1000"};
		SendLogCmd cmd = new SendLogCmd("wf1", wfid, null, getDefaultFS(), infoArray);
		cmd.sgProcess();
		
		//assertion
		//TODO
	}
	
	@Test
	public void testMsgGenRecieve() throws Exception {
		final String schemaFolder = "/test/sendmsg/";
		final String staticCfgName = "msggen.properties";
		final String schemaFile = "kafkaTimeTriggerMsg.schema";
		final String wfName="wfName1";
		final String wfid = "wf1";
		final int exeInterval=3;
		final int waitSeconds =10;
		
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
		
		ExecutorService es = Executors.newFixedThreadPool(3);
		es.submit(new Runnable(){
			@Override
			public void run() {
				ETLCmd cmd = new KafkaMsgGenCmd(wfName, wfid, getResourceSubFolder() +staticCfgName, getDefaultFS(), null);
				ETLCmdMain.exeCmd(cmd, exeInterval, waitSeconds);
			}
		});
		es.submit(new Runnable(){
			@Override
			public void run() {
				ETLCmd cmd = new KafkaMsgDecodeCmd(wfName, wfid, getResourceSubFolder() +staticCfgName, getDefaultFS(), null);
				ETLCmdMain.exeCmd(cmd, exeInterval, waitSeconds);
			}
		});
		es.awaitTermination(waitSeconds, TimeUnit.SECONDS);
		//assertion
		//TODO
	}
	
	@Test
	public void genMsgs() throws Exception{
		final String schemaFolder = "/test/sendmsg/";
		final String staticCfgName = "msggen.properties";
		final String schemaFile = "kafkaTimeTriggerMsg.schema";
		final String wfName = "wfName1";
		final String wfid = "wf1";
		final int waitSeconds = 5;
		
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
		
		ExecutorService es = Executors.newFixedThreadPool(3);
		es.submit(new Runnable(){
			@Override
			public void run() {
				ETLCmd cmd = new KafkaMsgGenCmd(wfName, wfid, getResourceSubFolder() +staticCfgName, getDefaultFS(), null);
				ETLCmdMain.exeCmd(cmd, waitSeconds, waitSeconds-2);
			}
		});
		es.awaitTermination(waitSeconds, TimeUnit.SECONDS);
		//assertion
		//TODO
	}
}
