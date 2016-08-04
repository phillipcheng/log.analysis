package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestCsvAggregateCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestCsvAggregateCmd.class);
	public static final String testCmdClass = "etl.cmd.transform.CsvAggregateCmd";

	public String getResourceSubFolder(){
		return "csvaggr/";
	}
	
	private void test1Fun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvaggregate1.properties";
			String[] csvFiles = new String[] {"csvaggregate.data"};
			
			List<String> output = super.mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false, ",");
			logger.info("Output is:"+output);
			
			// assertion
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(6);
			String[] csvs = sampleOutput.split(",");
			assertTrue("2.0".equals(csvs[6]));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void test1() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			test1Fun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					test1Fun();
					return null;
				}
			});
		}
	}
	
	private void testMultipleTablesFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvAggrMultipleFiles.properties";
			String[] csvFiles = new String[] {"MyCore_.data","MyCore1_.data"};
			
			List<String> output = super.mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false, ",");
			logger.info("Output is:"+output);
			
			// assertion
			assertTrue(output.size() == 4);
			/*
			String sampleOutput = output.get(6);
			String[] csvs = sampleOutput.split(",");
			assertTrue("2.0".equals(csvs[6]));
			*/
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testMultipleTables() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testMultipleTablesFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testMultipleTablesFun();
					return null;
				}
			});
		}
	}
}
