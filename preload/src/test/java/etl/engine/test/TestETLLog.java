package etl.engine.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.cmd.BackupCmd;
import etl.engine.LogType;
import etl.log.ETLLog;

public class TestETLLog {
	public static final Logger logger = LogManager.getLogger(TestETLLog.class);
	
	@Test
	public void test1() throws Exception {
		ETLLog etllog = new ETLLog(LogType.statistics);
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
		ETLLog etllog = new ETLLog(LogType.statistics);
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

}
