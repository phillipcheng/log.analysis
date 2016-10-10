package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import scala.Tuple2;

public class TestCsvSplitCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvSplitCmd.class);
	public static final String testTransformCmdClass="etl.cmd.CsvTransformCmd";
	public static final String testSplitCmdClass="etl.cmd.CsvSplitCmd";
	private static final String[] EMPTY_STRINGS = new String[0];

	public String getResourceSubFolder(){
		return "csvsplit/";
	}
	
	private void testCsvSplit() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvsplitinput/";
			String remoteCsvOutputFolder = "/etltest/csvsplit/";
			String csvtransProp = "csvtrans.properties";
			String[] csvFiles = new String[] {"A20160409.0000-20160409.0100_H_000000000_262227201_7CF85400035D_13.csv"};

			// 1. Transform
			List<String> output = mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testTransformCmdClass, false);
			logger.info("Output is:"+output);
			
			// assertion
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",", -1);
			int mergedColumn = 2;
			logger.info("Last element:"+csvs[csvs.length - 1]+" "+ csvs[mergedColumn] + " "+csvs[mergedColumn-1]+ " " +csvs[mergedColumn+1]);
			
			remoteCsvFolder = remoteCsvOutputFolder;
			remoteCsvOutputFolder = "/etltest/csvsplitoutput/";
			List<Tuple2<String, String[]>> remoteFolderInputfiles = new ArrayList<Tuple2<String, String[]>>();
			remoteFolderInputfiles.add(new Tuple2<String, String[]>(remoteCsvFolder, EMPTY_STRINGS));
			
			String csvsplitProp = "csvsplit.properties";
			// 2. Split
			output = mrTest(remoteCfgFolder, remoteFolderInputfiles, remoteCsvOutputFolder, csvsplitProp, testSplitCmdClass, false);
			logger.info("Output is:"+output);
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testFileNameRowValidationSkipHeader() throws Exception {
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
