package bdap.util.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;

public class TestHdfsUtil {
	public static final Logger logger = LogManager.getLogger(TestShowIps.class);
	@Test
	public void testGetContentsFromDfsFiles() throws Exception{
		String str = HdfsUtil.getContentsFromDfsFiles("hdfs://127.0.0.1:19000", "/abcd/");
		logger.info(String.format("contents:\n%s", str));
	}
}
