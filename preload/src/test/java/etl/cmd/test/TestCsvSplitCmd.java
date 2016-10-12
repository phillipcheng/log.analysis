package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class TestCsvSplitCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvSplitCmd.class);
	public static final String testCmdClass="etl.cmd.CsvSplitCmd";

	public String getResourceSubFolder(){
		return "csvsplit/";
	}
	
	private void testCsvSplit() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvsplitinput/";
			String remoteCsvOutputFolder = "/etltest/csvsplitoutput/";
			String csvtransProp = "csvsplit.properties";
			String[] csvFiles = new String[] {"part-r-00000.csv"};
			
			List<String> output = mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+output);
			
			// assertion
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",", -1);
			int mergedColumn = 2;
			logger.info("Last element:"+csvs[csvs.length - 1]+" "+ csvs[mergedColumn] + " "+csvs[mergedColumn-1]+ " " +csvs[mergedColumn+1]);
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testCsvSplitEntry() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testCsvSplit();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testCsvSplit();
					return null;
				}
			});
		}
	}
}
