package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.List;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;

public class TestCsvSplitCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvSplitCmd.class);
	public static final String testCmdClass="etl.cmd.CsvSplitCmd";

	public String getResourceSubFolder(){
		return "csvsplit/";
	}
	
	@Test
	public void testCsvSplit() throws Exception {
		String remoteCsvFolder = "/etltest/csvsplitinput/";
		String remoteCsvOutputFolder = "/etltest/csvsplitoutput/";
		String csvsplitProp = "csvsplit.properties";
		String[] csvFiles = new String[] {"part-r-00000.csv"};
		
		List<String> output = mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvsplitProp, csvFiles, testCmdClass, false);
		logger.info("Output is: {}", output);
		
		// assertion
		assertTrue(output.size() > 0);
		
		List<String> files = HdfsUtil.listDfsFile(getFs(), remoteCsvOutputFolder);
		logger.info("Output files: {}", files);
		assertEquals(files.size(), 4);
		assertTrue(output.contains("H,262227201,20160409.0000,20160409.0100,7CF85400035D,1233,1,262227201,0,0,0,0,0"));
	}
}
