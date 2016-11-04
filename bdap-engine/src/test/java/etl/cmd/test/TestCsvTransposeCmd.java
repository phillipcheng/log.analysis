package etl.cmd.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import etl.cmd.CsvTransposeCmd.TransposeSchema;

import static org.junit.Assert.*;

public class TestCsvTransposeCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvTransposeCmd.class);
	public static final String testCmdClass = "etl.cmd.CsvTransposeCmd";

	public String getResourceSubFolder(){
		return "csvtranspose/";
	}
	
	@Test
	public void testPDEStat() throws Exception {
		String remoteCsvFolder = "/etltest/csvtranspose/data/";
		String remoteCsvOutputFolder = "/etltest/csvtransposeoutput/";
		String csvtransProp = "csvTransposePDEStat.properties";
		String[] csvFiles = new String[] {"STAT1027.Small_AJ2"};		
		String schemaFolder = "/etltest/csvtranspose/cfg/"; //hardcoded in the properties
		String schemaFile = "pdeStatSchema.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		
		assertEquals(15,output.size());
		assertTrue(output.contains("20161027,0000,016,CTXKEY,0000000073,,,,,0000000007,,,"));

	}
	
	@Test
	public void testSingleColumn() throws Exception {
		String remoteCsvFolder = "/etltest/csvtranspose/data/";
		String remoteCsvOutputFolder = "/etltest/csvtransposeoutput/";
		String csvtransProp = "csvTransposeSimple.properties";
		String[] csvFiles = new String[] {"csvTransposeSimpleData.csv"};		
		String schemaFolder = "/etltest/csvtranspose/cfg/"; //hardcoded in the properties
		String schemaFile = "schemaSimple.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		assertEquals(3, output.size());
		assertTrue(output.contains("name1,A,B,D"));
		assertTrue(output.contains("name2,B,E,"));
		assertTrue(output.contains("name3,,F,C"));
	}
	
	@Test
	public void testMultipleColumn() throws Exception {
		String remoteCsvFolder = "/etltest/csvtranspose/data/";
		String remoteCsvOutputFolder = "/etltest/csvtransposeoutput/";
		String csvtransProp = "csvTransposeMultiple.properties";
		String[] csvFiles = new String[] {"csvTransposeMultipleData.csv"};		
		String schemaFolder = "/etltest/csvtranspose/cfg/"; //hardcoded in the properties
		String schemaFile = "schemaSimple.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		
		assertEquals(3, output.size());
		assertTrue(output.contains("name1,A,B,D,Very Good,Good,Not Bad"));
		assertTrue(output.contains("name2,B,E,,Good,Bad,"));
		assertTrue(output.contains("name3,,F,C,,Very Bad,Average"));
	}
	
	public void test(){
		//accept
				TransposeSchema ts=new TransposeSchema();
				Map<String, List<String>> a=new HashMap<String,List<String>>();
				a.put("table1", Arrays.asList(new String[]{"abc","def","fgh"}));
				a.put("table2", Arrays.asList(new String[]{"abc","def","fgh"}));
				ts.setTableMap(a);
				Map<String, List<String>> b=new HashMap<String,List<String>>();
				b.put("abc", Arrays.asList(new String[]{"111","222","333"}));
				b.put("abf", Arrays.asList(new String[]{"111","222","333"}));
				ts.setTypeMap(b);
				
				ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
				try {
					String json = ow.writeValueAsString(ts);
					logger.info("json:\n{}", json);
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
}
