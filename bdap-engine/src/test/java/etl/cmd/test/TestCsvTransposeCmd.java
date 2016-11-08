package etl.cmd.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class TestCsvTransposeCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvTransposeCmd.class);
	public static final String testCmdClass = "etl.cmd.CsvTransposeCmd";

	public String getResourceSubFolder(){
		return "csvtranspose/";
	}
	
	@Test
	public void testSingleColumn() throws Exception {
		String remoteCsvFolder = "/etltest/csvtranspose/input/";
		String remoteCsvOutputFolder = "/etltest/csvtranspose/output/";
		String csvtransProp = "singleColumn.properties";
		String[] csvFiles = new String[] {"singleColumnData.csv"};
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		assertEquals(3, output.size());
		assertTrue(output.contains("name1,A,B,D"));
		assertTrue(output.contains("name2,B,E,"));
		assertTrue(output.contains("name3,,F,C"));
	}
	
	@Test
	public void testMultipleColumns() throws Exception {
		String remoteCsvFolder = "/etltest/csvtranspose/input/";
		String remoteCsvOutputFolder = "/etltest/csvtranspose/output/";
		String csvtransProp = "multipleColumns.properties";
		String[] csvFiles = new String[] {"multipleColumnData.csv"};
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		assertEquals(2, output.size());
		assertTrue(output.contains("2016,g1,,v1_02,v1_03,v2_01,,v2_03"));
		assertTrue(output.contains("2016,g2,v1_01,,v1_03,v2_01,v2_02,"));
	}
	
	
	@Test
	public void testPDEStat() throws Exception {
		String remoteCsvFolder = "/etltest/csvtranspose/input/";
		String remoteCsvOutputFolder = "/etltest/csvtranspose/output/";
		String csvtransProp = "pdeStat.properties";
		String[] csvFiles = new String[] {"STAT1027.Small_AJ2"};		
		String cfgFolder = "/etltest/csvtranspose/cfg/"; //hardcoded in the properties

		String[] cfgFiles=new String[]{"pde_state_table_fields_mapping.properties","pde_state_table_name_mapping.properties"};
		for(String cfgFile:cfgFiles){
			getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+cfgFile), new Path(cfgFolder+cfgFile));
		}		
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		
		assertEquals(15,output.size());
		assertTrue(output.contains("20161027,0000,016,CTXKEY,0000000073,,,,,0000000007,,,"));

	}
}
