package etl.cmd.test;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.SendLogCmd;
import etl.engine.ETLCmdMain;

public class TestSendLogCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestSendLogCmd.class);
	
	private void testSendLogFun() throws Exception {
		String dfsFolder = "/test/sendlog/";
		String staticCfgName = "sendlog.properties";
		String wfid = "wf1";
		
		getFs().delete(new Path(dfsFolder), true);
		getFs().mkdirs(new Path(dfsFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsFolder + staticCfgName));
		
		// run cmd
		String[] infoArray = new String[]{"cmd1", "1000"};
		SendLogCmd cmd = new SendLogCmd(wfid, dfsFolder + staticCfgName, getDefaultFS(), infoArray);
		cmd.sgProcess();
	}
	
	@Test
	public void testSendLog() throws Exception{
		if (!super.isTestKafka()) return;
		if (getDefaultFS().contains("127.0.0.1")){
			testSendLogFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testSendLogFun();
					return null;
				}
			});
		}
	}
	
	private void testMsgGenRecieveFun() throws Exception {
		final String dfsFolder = "/test/sendmsg/";
		final String staticCfgName = "msggen.properties";
		final String schemaFile = "kafkaTimeTriggerMsg.schema";
		final String wfid = "wf1";
		final int waitSeconds = 20;
		
		getFs().delete(new Path(dfsFolder), true);
		getFs().mkdirs(new Path(dfsFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsFolder + staticCfgName));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(dfsFolder + schemaFile));
		
		ExecutorService es = Executors.newFixedThreadPool(3);
		es.submit(new Runnable(){
			@Override
			public void run() {
				ETLCmdMain.main(new String[]{"etl.cmd.KafkaMsgGenCmd", wfid, dfsFolder+staticCfgName, getDefaultFS(),
						String.format("%s=%s",ETLCmdMain.param_exe_interval, "4"),
						String.format("%s=%d",ETLCmdMain.param_exe_time, waitSeconds-2)
						});
				
			}
		});
		es.submit(new Runnable(){
			@Override
			public void run() {
				ETLCmdMain.main(new String[]{"etl.cmd.KafkaMsgDecodeCmd", wfid, dfsFolder+staticCfgName, getDefaultFS(),
						String.format("%s=%s",ETLCmdMain.param_exe_interval, "4"),
						String.format("%s=%d",ETLCmdMain.param_exe_time, waitSeconds-2)
						});
				
			}
		});
		es.awaitTermination(waitSeconds, TimeUnit.SECONDS);
	}
	
	@Test
	public void testMsgGenRecieve() throws Exception{
		if (!super.isTestKafka()) return;
		if (getDefaultFS().contains("127.0.0.1")){
			testMsgGenRecieveFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testMsgGenRecieveFun();
					return null;
				}
			});
		}
	}
	
	@Test
	public void genMsgs() throws Exception{
		if (!super.isTestKafka()) return;
		final String dfsFolder = "/test/sendmsg/";
		final String staticCfgName = "msggen.properties";
		final String schemaFile = "kafkaTimeTriggerMsg.schema";
		final String wfid = "wf1";
		final int waitSeconds = 200;
		
		getFs().delete(new Path(dfsFolder), true);
		getFs().mkdirs(new Path(dfsFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsFolder + staticCfgName));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(dfsFolder + schemaFile));
		
		ExecutorService es = Executors.newFixedThreadPool(3);
		es.submit(new Runnable(){
			@Override
			public void run() {
				ETLCmdMain.main(new String[]{"etl.cmd.KafkaMsgGenCmd", wfid, dfsFolder+staticCfgName, getDefaultFS(),
						String.format("%s=%s",ETLCmdMain.param_exe_interval, waitSeconds),
						String.format("%s=%d",ETLCmdMain.param_exe_time, waitSeconds-2)
						});
				
			}
		});
		es.awaitTermination(waitSeconds, TimeUnit.SECONDS);
	}

	@Override
	public String getResourceSubFolder() {
		return "sendmsg/";
	}
}
