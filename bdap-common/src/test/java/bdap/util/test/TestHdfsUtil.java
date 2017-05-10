package bdap.util.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;

public class TestHdfsUtil {
	public static final Logger logger = LogManager.getLogger(TestHdfsUtil.class);
	@Test
	public void testGetContentsFromDfsFiles() throws Exception{
//		/spc/dumpfile/rawinput/${wf:id()}/*
		String str = HdfsUtil.getContentsFromDfsFilesByPathFilter("hdfs://192.85.247.104:19000", "/spc/dumpfile/ftpout/0002084-170104195305044-oozie-dbad-W/*","*spc_lic_limits_*");
		logger.info(String.format("contents:\n%s", str));
	}
}