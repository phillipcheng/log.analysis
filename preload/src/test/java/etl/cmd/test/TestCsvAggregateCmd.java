package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.engine.SafeSimpleDateFormat;
import etl.util.GroupFun;

public class TestCsvAggregateCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestCsvAggregateCmd.class);
	public static final String testCmdClass = "etl.cmd.CsvAggregateCmd";

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
			
			List<String> output = super.mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+output);
			
			// assertion
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(6);
			String[] csvs = sampleOutput.split(",", -1);
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
			List<String> output = super.mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+output);
			// assertion
			assertTrue(output.size() == 4);
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
	
	private void groupFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvAggrGroupFun1.properties";
			String[] csvFiles = new String[] {"MyCore_.data"};
			List<String> output = super.mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+String.join("\n", output));
			// assertion
			assertTrue(output.size()==4);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testGroupFun() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			groupFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					groupFun();
					return null;
				}
			});
		}
	}
	
	private void mergeTables() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvAggrMergeTables.properties";
			String[] csvFiles = new String[] {"MyCore_.data", "MyCore1_.data"};
			List<String> output = super.mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+String.join("\n", output));
			String dt = "2016-03-09T07:50:00+00:00";
			String dtformat = "yyyy-MM-dd'T'HH:mm:ssXXX";
			String hour = GroupFun.hour(dt, dtformat);
			String day = GroupFun.day(dt, dtformat);
			String line = String.format("%s,%s,PT300S,vQDSD0101SGS-L-AL-20,lcp-1,vQDSD0101SGS-L-AL-20-VLR-01,1.0,36049.0,32907.0,36049.0,32907.0"
					,hour,day);
			assertTrue(output.contains(line));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testMergeTables() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			mergeTables();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					mergeTables();
					return null;
				}
			});
		}
	}
	
	private void mergeTablesOuterjoin() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvAggrMergeTablesOuterjoin.properties";
			String[] csvFiles = new String[] {"MyCore_.do", "MyCore1_.do"};
			List<String> output = super.mrTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+String.join("\n", output));
			String dt = "2016-03-28T11:05:00+00:00";
			String dtformat = "yyyy-MM-dd'T'HH:mm:ssXXX";
			String hour = GroupFun.hour(dt, dtformat);
			String day = GroupFun.day(dt, dtformat);
			String csv=String.format("%s,%s,PT300S,QDSD0101vSGS-L-NK-20,lcp-1,QDSD0101vSGS-L-NK-20-VLR-00,0.0,0.0,0.0,114258.0,114258.0",
					hour, day);
			assertTrue(output.contains(csv));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testMergeTablesOuterjoin() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			mergeTablesOuterjoin();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					mergeTablesOuterjoin();
					return null;
				}
			});
		}
	}
}
