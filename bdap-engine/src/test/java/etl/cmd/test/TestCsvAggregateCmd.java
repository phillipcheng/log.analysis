package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.util.GroupFun;

public class TestCsvAggregateCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvAggregateCmd.class);
	public static final String testCmdClass = "etl.cmd.CsvAggregateCmd";

	public String getResourceSubFolder(){
		return "csvaggr/";
	}
	
	@Test
	public void testNoSchema() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "csvAggrNoSchema.properties";
		String[] csvFiles = new String[] {"csvaggregate.data"};
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:"+output);
		
		// assertion
		assertTrue(output.size() > 0);
		String sampleOutput = output.get(6);
		String[] csvs = sampleOutput.split(",", -1);
		assertTrue("2.0".equals(csvs[6]));
	}
	
	@Test
	public void multipleTablesNoMerge() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "multipleTablesNoMerge.properties";
		String[] csvFiles = new String[] {"MyCore_.data","MyCore1_.data"};
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n"+ String.join("\n", output));
		// assertion
		assertTrue(output.size() == 4);
	}
	
	@Test
	public void testGroup() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "csvAggrGroupFun1.properties";
		String[] csvFiles = new String[] {"MyCore_.data"};
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:"+String.join("\n", output));
		// assertion
		assertTrue(output.size()==4);
	}
	
	@Test
	public void mergeIntoOneTable() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "csvAggrMergeTablesOuterjoin.properties";
		String[] csvFiles = new String[] {"MyCore_.do", "MyCore1_.do"};
		//prepare schema
		String schemaFolder = "/etltest/aggr/cfg/"; //hardcoded in the properties
		String schemaFile = "multipleTableSchemas.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n"+String.join("\n", output));
		String dt = "2016-03-28T11:05:00+00:00";
		String dtformat = "yyyy-MM-dd'T'HH:mm:ssXXX";
		String hour = GroupFun.hour(dt, dtformat);
		String day = GroupFun.day(dt, dtformat);
		String csv=String.format("%s,%s,PT300S,QDSD0101vSGS-L-NK-20,lcp-1,QDSD0101vSGS-L-NK-20-VLR-00,0.0,0.0,0.0,114258.0,114258.0",
				hour, day);
		assertTrue(output.contains(csv));
	}
	
	@Test
	public void testNoGroupAggr() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "csvAggrNoGroup.properties";
		String[] csvFiles = new String[] {"data1.data"};
		//prepare data
		String dataFile = "maxA";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+dataFile), new Path("/data/"+dataFile));
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:"+output);
		
		// assertion
		assertTrue(output.size() > 0);
		String sampleOutput = output.get(0);
		String[] csvs = sampleOutput.split(",", -1);
		assertTrue("1.0".equals(csvs[0]));
	}
	
	@Test
	public void mergeIntoMultiple() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "mergeIntoMultiple.properties";
		String[] csvFiles = new String[] {"MyCore_.do", "MyCore1_.do"};
		//prepare schema
		String schemaFolder = "/etltest/aggr/cfg/"; //hardcoded in the properties
		String schemaFile = "multipleTableSchemas.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		List<String> files = HdfsUtil.listDfsFile(super.getFs(), remoteCsvOutputFolder);
		assertTrue(files.contains("MyCoreMerge1-r-00000"));
		assertTrue(files.contains("MyCoreMerge2-r-00000"));
		logger.info("Output is:\n"+String.join("\n", output));
		String dt = "2016-03-28T11:05:00+00:00";
		String dtformat = "yyyy-MM-dd'T'HH:mm:ssXXX";
		String hour = GroupFun.hour(dt, dtformat);
		String day = GroupFun.day(dt, dtformat);
		String csv=String.format("%s,%s,PT300S,QDSD0101vSGS-L-NK-20,lcp-1,QDSD0101vSGS-L-NK-20-VLR-00,0.0,0.0,0.0,114258.0,114258.0",
				hour, day);
		assertTrue(output.contains(csv));
	}
	
}
