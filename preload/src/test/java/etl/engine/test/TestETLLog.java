package etl.engine.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.BackupCmd;
import etl.engine.ETLLog;

public class TestETLLog {
	public static final Logger logger = Logger.getLogger(TestETLLog.class);
	
	@Test
	public void test1() throws Exception {
		ETLLog etllog = new ETLLog();
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
		String expected = "2016-05-16T12:10:15.123,2016-05-16T12:11:15.123,,,etl.cmd.BackupCmd,123,122,,";
		logger.info(str);
		assertTrue(expected.equals(str));
	}
	
	@Test
	public void test2() throws Exception {
		ETLLog etllog = new ETLLog();
		//"yyyy-MM-ddTHH:mm:ss.SSS"
		String startDate = "2016-05-16T12:10:15.123";
		etllog.setStart(ETLLog.ssdf.parse(startDate));
		String endDate = "2016-05-16T12:11:15.123";
		etllog.setEnd(ETLLog.ssdf.parse(endDate));
		etllog.setActionName(BackupCmd.class.getName());
		String str = etllog.toString();
		String expected = "2016-05-16T12:10:15.123,2016-05-16T12:11:15.123,,,etl.cmd.BackupCmd,,,,";
		logger.info(str);
		assertTrue(expected.equals(str));
	}

}
