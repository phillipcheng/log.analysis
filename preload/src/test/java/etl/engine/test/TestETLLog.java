package etl.engine.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import etl.cmd.BackupCmd;
import etl.cmd.KafkaMsgGenCmd;
import etl.cmd.test.TestETLCmd;
import etl.engine.ETLCmd;
import etl.engine.ETLCmdMain;
import etl.engine.EngineUtil;
import etl.log.ETLLog;
import etl.log.LogType;
import etl.log.StreamLogProcessor;

public class TestETLLog extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestETLLog.class);
	
	@Test
	public void test1() throws Exception {
		ETLLog etllog = new ETLLog(LogType.etlstat);
		//"yyyy-MM-ddTHH:mm:ss.SSS"
		String startDate = "2016-05-16T12:10:15.123";
		etllog.setStart(ETLLog.ssdf.parse(startDate));
		String endDate = "2016-05-16T12:11:15.123";
		etllog.setEnd(ETLLog.ssdf.parse(endDate));
		etllog.setActionName(BackupCmd.class.getName());
		List<String> logInfo = new ArrayList<String>();
		logInfo.add("123");
		logInfo.add("122");
		etllog.setCounts(logInfo);
		String str = etllog.toString();
		String expected = "statistics,2016-05-16T12:10:15.123,2016-05-16T12:11:15.123,,,etl.cmd.BackupCmd,123,122,,";
		logger.info(str);
		assertTrue(expected.equals(str));
	}
	
	@Test
	public void test2() throws Exception {
		ETLLog etllog = new ETLLog(LogType.etlstat);
		//"yyyy-MM-ddTHH:mm:ss.SSS"
		String startDate = "2016-05-16T12:10:15.123";
		etllog.setStart(ETLLog.ssdf.parse(startDate));
		String endDate = "2016-05-16T12:11:15.123";
		etllog.setEnd(ETLLog.ssdf.parse(endDate));
		etllog.setActionName(BackupCmd.class.getName());
		String str = etllog.toString();
		String expected = "statistics,2016-05-16T12:10:15.123,2016-05-16T12:11:15.123,,,etl.cmd.BackupCmd,,,,";
		logger.info(str);
		assertTrue(expected.equals(str));
	}
	
	@Test
	public void genLog() throws Exception{
		
		final String msggenCfgFolder= "/test/sendmsg/";
		final String msggneCfgName = "msggen.properties";
		
		final String msggenWfName = "msggenWf";
		final String msggenWfId = "msggenWfId";
		final int exeInterval = 2;
		final int totalExeTime = 200;
		
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + msggneCfgName), new Path(msggenCfgFolder + msggneCfgName));
		getFs().copyFromLocalFile(false, true, new Path("src/main/resources/logschema.txt"), 
				new Path(EngineUtil.getInstance().getEngineProp().getString("log.schema.file")));
		
		ExecutorService es = Executors.newFixedThreadPool(5);
		es.submit(new Runnable(){
			@Override
			public void run() {
				ETLCmd cmd = new KafkaMsgGenCmd(msggenWfName, msggenWfId, msggenCfgFolder+msggneCfgName, getDefaultFS(), null);
				cmd.setSendLog(false);
				ETLCmdMain.exeCmd(cmd, exeInterval, totalExeTime-2);
			}
		});
		
		es.submit(new Runnable(){
			@Override
			public void run() {
				SparkConf conf = new SparkConf().setAppName("mtccore").setMaster("local[3]");
				final JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
				StreamLogProcessor.sparkProcess(jsc);
			}
		});
		
		es.awaitTermination(totalExeTime, TimeUnit.SECONDS);
	}

	@Override
	public String getResourceSubFolder() {
		return "etllog/";
	}

}
