package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.List;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.util.GroupFun;

public class TestCsvAggregateCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvAggregateCmd.class);
	public static final String testCmdClass = "etl.cmd.CsvAggregateCmd";

	public String getResourceSubFolder(){
		return "csvaggr/";
	}
	
	@Test
	public void test1() throws Exception {
		try {
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvaggregate1.properties";
			String[] csvFiles = new String[] {"csvaggregate.data"};
			
			List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
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
	public void testMultipleTables() throws Exception {
		try {
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvAggrMultipleFiles.properties";
			String[] csvFiles = new String[] {"MyCore_.data","MyCore1_.data"};
			List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+output);
			// assertion
			assertTrue(output.size() == 4);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testGroup() throws Exception {
		try {
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvAggrGroupFun1.properties";
			String[] csvFiles = new String[] {"MyCore_.data"};
			List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+String.join("\n", output));
			// assertion
			assertTrue(output.size()==4);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void mergeTables() throws Exception {
		try {
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvAggrMergeTables.properties";
			String[] csvFiles = new String[] {"MyCore_.data", "MyCore1_.data"};
			List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
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
	public void mergeTablesOuterjoin() throws Exception {
		try {
			String remoteCsvFolder = "/etltest/csvaggr/";
			String remoteCsvOutputFolder = "/etltest/csvaggrout/";
			String csvtransProp = "csvAggrMergeTablesOuterjoin.properties";
			String[] csvFiles = new String[] {"MyCore_.do", "MyCore1_.do"};
			List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
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
	
}
