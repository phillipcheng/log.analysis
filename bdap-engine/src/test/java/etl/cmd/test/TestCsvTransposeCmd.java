package etl.cmd.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import scala.Tuple2;

public class TestCsvTransposeCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvTransposeCmd.class);
	public static final String testCmdClass = "etl.cmd.CsvTransposeCmd";

	public String getResourceSubFolder(){
		return "csvtranspose/";
	}
	
	@Test
	public void testSingleColumn() throws Exception {
		String inputFolder = "/etltest/csvtranspose/input/";
		String outputFolder = "/etltest/csvtranspose/output/";
		String cfgFolder = "/etltest/csvtranspose/cfg/";
		
		String csvtransProp = "singleColumn.properties";
		String schemaFileName = "singleColumn.schema";
		
		String[] csvFiles = new String[] {"singleColumnData.csv"};
		
		getFs().delete(new Path(cfgFolder), true);
		getFs().mkdirs(new Path(cfgFolder));
		
		//copy schema file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFileName), new Path(cfgFolder + schemaFileName));
		
		List<String> output = super.mrTest(inputFolder, outputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		assertEquals(3, output.size());
		assertTrue(output.contains("name1,A,B,D"));
		assertTrue(output.contains("name2,B,E,"));
		assertTrue(output.contains("name3,,F,C"));
	}
	
	@Test
	public void testPDEStat() throws Exception {
		String inputFolder = "/etltest/csvtranspose/input/";
		String outputFolder = "/etltest/csvtranspose/output/";
		String cfgFolder = "/etltest/csvtranspose/cfg/";
		
		String csvtransProp = "pdeStat.properties";
		String schemaFileName = "stat.schema";
		String[] csvFiles = new String[] {"STAT1027.Small_AJ2"};	
		
		getFs().delete(new Path(cfgFolder), true);
		getFs().mkdirs(new Path(cfgFolder));
		
		//copy schema file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFileName), new Path(cfgFolder + schemaFileName));
		
		List<String> output = super.mrTest(inputFolder, outputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		
		assertEquals(25,output.size());
		assertTrue(output.contains("20161027,0000,016,CTXKEY,0000000073,0000000007,"));
	}
	
	@Test
	public void testPDEStatSpark() throws Exception {
		String inputFolder = "/etltest/csvtranspose/input/";
		String cfgFolder = "/etltest/csvtranspose/cfg/";
		
		String csvtransProp = "pdeStat.properties";
		String schemaFileName = "stat.schema";
		String[] csvFiles = new String[] {"STAT1027.Small_AJ2"};	
		
		getFs().delete(new Path(cfgFolder), true);
		getFs().mkdirs(new Path(cfgFolder));
		
		//copy schema file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFileName), new Path(cfgFolder + schemaFileName));
		
		Tuple2<List<String>,List<String>> ret = super.sparkTestKV(inputFolder, csvFiles, csvtransProp, 
				etl.cmd.CsvTransposeCmd.class, TextInputFormat.class);
		List<String> output = ret._2;
		logger.info("Output is:\n" + String.join("\n", output));
		
		assertEquals(25,output.size());
		assertTrue(output.contains("20161027,0000,016,CTXKEY,0000000073,0000000007,"));
	}
}
