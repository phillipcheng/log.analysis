package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;

public class TestUnpackCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestUnpackCmd.class);
	public static final String testCmdClass="etl.cmd.UnpackCmd";

	public String getResourceSubFolder() {
		return "unpack/";
	}

	@Test
	public void testUnpack() throws Exception {
		String remoteCsvInputFolder = "/etltest/unpack/input/";
		String remoteCsvOutputFolder = "/etltest/unpack/output/";
		String unpackProp = "unpack.properties";
		String[] csvFiles = new String[]{"20160409_BB1.zip", "test.csv"};

		getFs().delete(new Path("/etltest/unpack/output/20160409_BB1"), true);
		
		List<String> output = super.mapTest(remoteCsvInputFolder, remoteCsvOutputFolder, unpackProp, csvFiles, testCmdClass, true);
		logger.info("Output is:"+output);
		
		List<String> fl = HdfsUtil.listDfsFile(getFs(), "/etltest/unpack/output/20160409_BB1/01");
		//assert
		logger.info(fl);
		assertTrue(fl.contains("A20160409.0000-20160409.0100_E_000000000_262216704_F4D9FB75EA47_13.csv"));
		
		fl = HdfsUtil.listDfsFile(getFs(), "/etltest/unpack/output/20160409_BB1");
		logger.info(fl);
		assertTrue(fl.contains("test.txt"));
		assertTrue(!fl.contains("test.dat"));
	}
	
}
