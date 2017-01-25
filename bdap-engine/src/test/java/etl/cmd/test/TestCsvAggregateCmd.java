package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.CsvAggregateCmd;
import etl.engine.LogicSchema;
import etl.util.GroupFun;
import etl.util.StringUtil;
import scala.Tuple2;

public class TestCsvAggregateCmd extends TestETLCmd {
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = LogManager.getLogger(TestCsvAggregateCmd.class);
	public static final String testCmdClass = "etl.cmd.CsvAggregateCmd";

	public String getResourceSubFolder(){
		return "csvaggr/";
	}
	
	@Test
	public void testLeftjoin() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "csvaggr.mergetable.leftjoin.properties";
		String[] csvFiles = new String[] {"femto-r-00000", "RRC_connection_establishments-r-00000"};
		//prepare schema
		String cfgFolder = "/etltest/aggr/cfg/"; //hardcoded in the properties
		String schemaFile = "om_map_merged.schema";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(cfgFolder+schemaFile));
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n"+String.join("\n", output));
		assertEquals(4, output.size());
		assertTrue(output.contains("000003FE234C,A,E,2016-12-01 10:00:00.000,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12,12,0,0,0,0,10,0,0,0,0,0,EOF,2016-12-01 03:30:01.000,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056,105,106,13,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ERIC"));
		assertTrue(output.contains("000003FE234C,A,E,2016-12-12 09:00:00.000,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12,12,0,0,0,0,0,0,0,0,0,0,EOF,2016-12-12 03:30:01.000,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056,105,106,13,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ALU"));
		assertTrue(output.contains("000003FE234C,A,E,2016-12-12 10:00:00.000,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12,12,0,0,0,0,10,0,0,0,0,0,EOF,2016-12-12 03:30:01.000,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056,105,106,13,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ALU"));
		assertTrue(output.contains("71DB021D7868,A,E,2016-12-12 11:00:00.000,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12,12,0,0,0,0,0,0,0,0,0,0,EOF,,,,,,,,,,,,,,,,,,,,,"));
	}
	
	@Test
	public void testLeftJoinSpark() throws Exception{
		String remoteInputFolder = "/etltest/csvaggr/";
		String csvtransProp = "csvaggr.mergetable.leftjoin.properties";
		String[] csvFiles = new String[] {"femto-r-00000", "RRC_connection_establishments-r-00000"};
		//prepare schema
		String cfgFolder = "/etltest/aggr/cfg/"; //hardcoded in the properties
		String schemaFile = "om_map_merged.schema";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(cfgFolder+schemaFile));
		
		List<String> ret = super.sparkTestKV(remoteInputFolder, csvFiles, csvtransProp, 
				etl.cmd.CsvAggregateCmd.class, TextInputFormat.class);
		//assertion
		logger.info("Output is:\n"+String.join("\n", ret));
		assertEquals(4, ret.size());
		assertTrue(ret.contains("000003FE234C,A,E,2016-12-01 10:00:00.000,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12,12,0,0,0,0,10,0,0,0,0,0,EOF,2016-12-01 03:30:01.000,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056,105,106,13,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ERIC"));
		assertTrue(ret.contains("000003FE234C,A,E,2016-12-12 09:00:00.000,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12,12,0,0,0,0,0,0,0,0,0,0,EOF,2016-12-12 03:30:01.000,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056,105,106,13,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ALU"));
		assertTrue(ret.contains("000003FE234C,A,E,2016-12-12 10:00:00.000,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12,12,0,0,0,0,10,0,0,0,0,0,EOF,2016-12-12 03:30:01.000,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056,105,106,13,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ALU"));
		assertTrue(ret.contains("71DB021D7868,A,E,2016-12-12 11:00:00.000,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12,12,0,0,0,0,0,0,0,0,0,0,EOF,,,,,,,,,,,,,,,,,,,,,"));
	}
	
	@Test
	public void noSchemaSum() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "NoSchemaSum.properties";
		String[] csvFiles = new String[] {"csvaggregate.csv"};
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n"+ String.join("\n", output));
		
		// assertion
		assertTrue(output.size() ==12);
		String sampleOutput = output.get(6);
		String[] csvs = sampleOutput.split(",", -1);
		assertTrue("2.0".equals(csvs[6]));
	}
	
	@Test
	public void noSchemaSumSpark() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String csvtransProp = "NoSchemaSum.properties";
		String[] csvFiles = new String[] {"csvaggregate.csv"};
		
		List<String> ret = super.sparkTestKV(remoteCsvFolder, csvFiles, csvtransProp, etl.cmd.CsvAggregateCmd.class, 
				TextInputFormat.class);
		Collections.sort(ret);
		logger.info("Output is:\n"+ String.join("\n", ret));
		
		// assertion
		assertTrue(ret.size() ==12);
		List<String> col = StringUtil.getColumn(ret, 6);
		assertTrue(col.contains("2.0"));
	}
	
	@Test
	public void noSchemaMax() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "NoSchemaMax.properties";
		String[] csvFiles = new String[] {"csvaggregate.csv"};
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n"+ String.join("\n", output));
		
		// assertion
		assertTrue(output.size() ==12);
		String sampleOutput = output.get(6);
		String[] csvs = sampleOutput.split(",", -1);
		logger.info(csvs[8]);
		assertTrue("56193.0".equals(csvs[8]));
	}
	
	@Test
	public void noSchemaCount() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "NoSchemaCount.properties";
		String[] csvFiles = new String[] {"csvaggregate.csv"};
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n"+ String.join("\n", output));
		
		// assertion
		assertTrue(output.size() ==12);
		String sampleOutput = output.get(6);
		String[] csvs = sampleOutput.split(",", -1);
		assertTrue(csvs[csvs.length-1].equals("2"));
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
	public void testGroupFun() throws Exception {
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
		logger.info("Excpet has:{}",csv);
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
	
	@Test
	public void testCsvCount() throws Exception {
		String staticCfgName = "csvcount.properties";
		String inputFolder = "/test/csvcount/input/";
		String outputFolder = "/test/csvcount/output/";
		String csvFileName1 = "sample1.csv";
		String csvFileName2 = "sample2.csv";
		
		String[] inputFiles = new String[]{csvFileName1,csvFileName2};
		
		//copy csv file
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName1), new Path(inputFolder + csvFileName1));
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName2), new Path(inputFolder + csvFileName2));
		
		List<String> output = super.mrTest(inputFolder, outputFolder, staticCfgName, inputFiles, testCmdClass, false);
		logger.info("Output is:\n"+String.join("\n",output));
		
		Assert.assertEquals(5, output.size());
		Assert.assertTrue(output.contains("Name1,A,2"));
		Assert.assertTrue(output.contains("Name1,B,1"));
		Assert.assertTrue(output.contains("Name1,E,1"));
		Assert.assertTrue(output.contains("Name2,B,2"));
		Assert.assertTrue(output.contains("Name2,D,2"));
	}
	
	@Test
	public void testCsvSplitCount() throws Exception{
		String staticCfgName = "csvcount.split.properties";
		String inputFolder = "/test/csvcount/input/";
		String outputFolder = "/test/csvcount/output/";
		String csvFileName1 = "sample3.csv";
		String csvFileName2 = "sample4.csv";
		
		String[] inputFiles = new String[]{csvFileName1,csvFileName2};
		//copy csv file
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName1), new Path(inputFolder + csvFileName1));
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName2), new Path(inputFolder + csvFileName2));
		
		List<String> output = super.mrTest(inputFolder, outputFolder, staticCfgName, inputFiles, testCmdClass, false);
		logger.info("Output is:\n"+String.join("\n",output));
		
		Assert.assertEquals(5, output.size());
		Assert.assertEquals("2016-10-12 10:10:00.0,2016-10-12 10:10:04.999,1", output.get(0));
		Assert.assertEquals("2016-10-12 10:10:05.0,2016-10-12 10:10:09.999,1", output.get(1));
		Assert.assertEquals("2016-10-12 10:10:10.0,2016-10-12 10:10:14.999,1", output.get(2));
		Assert.assertEquals("2016-10-12 10:10:15.0,2016-10-12 10:10:19.999,2", output.get(3));
		Assert.assertEquals("2016-10-12 10:10:20.0,2016-10-12 10:10:24.999,1", output.get(4));
	}
	
	@Test
	public void testMergeTableCheckOrder() throws Exception{
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "csvAggrMergeTablesOuterjoinCheckOrder.properties";
		String[] csvFiles = new String[] {"MyCoreA.do", "MyCoreZ.do"};
		//prepare schema
		String schemaFolder = "/etltest/aggr/cfg/"; //hardcoded in the properties
		String schemaFile = "multipleTableCheckOrderSchemas.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n"+String.join("\n", output));
		String dt = "2016-03-28T11:05:00+00:00";
		String dtformat = "yyyy-MM-dd'T'HH:mm:ssXXX";
		String hour = GroupFun.hour(dt, dtformat);
		String day = GroupFun.day(dt, dtformat);
		String csv=String.format("%s,%s,PT300S,QDSD0101vSGS-L-NK-20,lcp-1,QDSD0101vSGS-L-NK-20-VLR-00,114258.0,114258.0,1.0,114258.0,114258.0",
				hour, day);
		assertTrue(output.contains(csv));
		
		CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", "wf1", this.getResourceSubFolder() + csvtransProp, getDefaultFS(), null);
		cmd.sgProcess();
		LogicSchema ls = cmd.getLogicSchema();
		logger.info(String.format("ls:%s", ls));
		List<String> mergeAttrNames = ls.getAttrNameMap().get("MyCoreAZMerge_");
		String expectedNames = "[endTimeHour, endTimeDay, duration, SubNetwork, ManagedElement, Machine, MyCoreA_VS.avePerCoreCpuUsage, "
				+ "MyCoreA_VS.peakPerCoreCpuUsage, MyCoreZ_MyCore, MyCoreZ_VS.avePerCoreCpuUsage, MyCoreZ_VS.peakPerCoreCpuUsage]";
		assertTrue(expectedNames.equals(mergeAttrNames.toString()));
	}
	
	@Test
	public void noSchemaSumFiterGroupKeys() throws Exception {
		String remoteCsvFolder = "/etltest/csvaggr/";
		String remoteCsvOutputFolder = "/etltest/csvaggrout/";
		String csvtransProp = "NoSchemaSumFilterGroupKeys.properties";
		String[] csvFiles = new String[] {"csvaggregate.csv"};
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n"+ String.join("\n", output));
		
		// assertion
		assertTrue(output.size() ==12);
		String sampleOutput = output.get(6);
		String[] csvs = sampleOutput.split(",", -1);
		assertTrue("2.0".equals(csvs[1]));
	}
	
}
