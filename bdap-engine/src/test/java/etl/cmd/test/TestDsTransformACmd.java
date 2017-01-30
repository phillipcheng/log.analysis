package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import bdaps.engine.cmd.DatasetSqlCmd;
import etl.engine.InputFormatType;
import etl.util.GroupFun;
import etl.util.StringUtil;

public class TestDsTransformACmd extends TestETLCmd {
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = LogManager.getLogger(TestDsTransformACmd.class);

	public String getResourceSubFolder(){
		return "dstransform/";
	}
	
	@Test
	public void testLeftJoinSpark() throws Exception{
		String remoteInputFolder = "/etltest/dssql_data/";//hardcoded in the properties
		String csvtransProp = "mergetable.leftjoin.properties";
		String[] csvFiles = new String[] {"femto-r-00000", "RRC_connection_establishments-r-00000"};
		//prepare schema
		String cfgFolder = "/etltest/dssql_schema/"; 
		String schemaFile = "om_map_merged.schema";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(cfgFolder+schemaFile));
		
		List<String> output = super.sparkTestACmd(remoteInputFolder, csvFiles, csvtransProp, 
				DatasetSqlCmd.class, InputFormatType.Text);
		//assertion
		logger.info("Output is:\n"+String.join("\n", output));
		assertEquals(4, output.size());
		assertTrue(output.contains("000003FE234C,A,E,2016-12-12 09:00:00.0,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12.0,12.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0,2016-12-12 03:30:01.0,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59.0,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056.0,105.0,106.0,13.0,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ALU"));
		assertTrue(output.contains("000003FE234C,A,E,2016-12-12 10:00:00.0,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12.0,12.0,0.0,0.0,0.0,0.0,10.0,0.0,0.0,0.0,0.0,0.0,0,2016-12-12 03:30:01.0,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59.0,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056.0,105.0,106.0,13.0,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ALU"));
		assertTrue(output.contains("000003FE234C,A,E,2016-12-01 10:00:00.0,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12.0,12.0,0.0,0.0,0.0,0.0,10.0,0.0,0.0,0.0,0.0,0.0,0,2016-12-01 03:30:01.0,BBTPNJ33-FDB-01-2,000003FE234C,BBTPNJ33-FDB-01-2,59.0,InService,25027,311480-0E74101,07920,1.0.0.21,2016-09-28 03:29:38.0,64056.0,105.0,106.0,13.0,42.381736,-71.932083,MA,eFemto,311480-FA12E1C,ERIC"));
		assertTrue(output.contains("71DB021D7868,A,E,2016-12-12 11:00:00.0,2016-12-12 10:00:00.0,0,262216706,13,0,0,2,0,0,0,12.0,12.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0,,,,,0,,,,,,,0,0,0,0,,,,,,"));
	}
	
	@Test
	public void noSchemaSumSpark() throws Exception {
		String remoteCsvFolder = "/etltest/dstransform/";
		String csvtransProp = "NoSchemaSum.properties";
		String[] csvFiles = new String[] {"csvaggregate.csv"};
		
		List<String> ret = super.sparkTestACmd(remoteCsvFolder, csvFiles, csvtransProp, 
				DatasetSqlCmd.class, InputFormatType.Text);
		ArrayList<String> output = new ArrayList<String>();
		output.addAll(ret);
		Collections.sort(output);
		logger.info("Output is:\n"+ String.join("\n", output));
		
		// assertion
		assertTrue(output.size() ==12);
		List<String> col = StringUtil.getColumn(output, 6);
		assertTrue(col.contains("2.0"));
	}
	
	@Test
	public void noSchemaMax() throws Exception {
		String remoteCsvFolder = "/etltest/dstransform/";
		String csvtransProp = "NoSchemaMax.properties";
		String[] csvFiles = new String[] {"csvaggregate.csv"};
		
		List<String> output = super.sparkTestACmd(remoteCsvFolder, csvFiles, csvtransProp, DatasetSqlCmd.class, InputFormatType.Text);
		logger.info("Output is:\n"+ String.join("\n", output));
		
		// assertion
		assertTrue(output.size() ==12);
		List<String> col = StringUtil.getColumn(output, 8);
		assertTrue(col.contains("56193.0"));
	}
	
	@Test
	public void noSchemaCount() throws Exception {
		String remoteCsvFolder = "/etltest/dstransform/";
		String csvtransProp = "NoSchemaCount.properties";
		String[] csvFiles = new String[] {"csvaggregate.csv"};
		
		List<String> output = super.sparkTestACmd(remoteCsvFolder, csvFiles, csvtransProp, DatasetSqlCmd.class, InputFormatType.Text);
		logger.info("Output is:\n"+ String.join("\n", output));
		
		// assertion
		assertTrue(output.size() ==12);
		int cnt = output.get(0).split(",",-1).length;
		List<String> col = StringUtil.getColumn(output, cnt-1);
		assertTrue(col.contains("2"));
	}
	
	@Test
	public void multipleTablesNoMerge() throws Exception {
		String remoteCsvFolder = "/etltest/dstransform/";
		String csvtransProp = "multipleTablesNoMerge.properties";
		String[] csvFiles = new String[] {"MyCore_.data","MyCore1_.data"};
		//prepare schema
		String cfgFolder = "/etltest/cfg/"; //referenced from properties
		String schemaFile = "multipleTableSchemas.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(cfgFolder+schemaFile));
		
		List<String> output = super.sparkTestACmd(remoteCsvFolder, csvFiles, csvtransProp, DatasetSqlCmd.class, InputFormatType.Text);
		logger.info("Output is:\n"+ String.join("\n", output));
		// assertion
		assertTrue(output.size() == 4);
	}
	
	@Test
	public void testTimestampFormat() throws Exception {
		String remoteCsvFolder = "/etltest/dstransform/";
		String csvtransProp = "csvAggrGroupFun1.properties";
		String[] csvFiles = new String[] {"MyCore_.data"};
		//prepare schema
		String cfgFolder = "/etltest/cfg/"; //referenced from properties
		String schemaFile = "dynschema_test1_schemas.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(cfgFolder+schemaFile));
		//
		List<String> output = super.sparkTestACmd(remoteCsvFolder, csvFiles, csvtransProp, DatasetSqlCmd.class, InputFormatType.Text);
		logger.info("Output is:\n"+String.join("\n", output));
		// assertion
		assertTrue(output.size()==4);
		List<String> col = StringUtil.getColumn(output, 0);
		assertTrue(col.contains("23"));
	}
	
	@Test
	public void mergeIntoOneTable() throws Exception {
		String remoteCsvFolder = "/etltest/dstransform/";
		String csvtransProp = "csvAggrMergeTablesOuterjoin.properties";
		String[] csvFiles = new String[] {"MyCore_.do", "MyCore1_.do"};
		//prepare schema
		String schemaFolder = "/etltest/aggr/cfg/"; //hardcoded in the properties
		String schemaFile = "multipleTableSchemas.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.sparkTestACmd(remoteCsvFolder, csvFiles, csvtransProp, DatasetSqlCmd.class, InputFormatType.Text);
		logger.info("Output is:\n"+String.join("\n", output));
		String dt = "2016-03-28T11:05:00+00:00";
		String dtformat = "yyyy-MM-dd'T'HH:mm:ssXXX";
		String hour = GroupFun.hour(dt, dtformat);
		String day = GroupFun.day(dt, dtformat);
		String csv=String.format("%s,%s,PT300S,QDSD0101vSGS-L-NK-20,lcp-1,QDSD0101vSGS-L-NK-20-VLR-00,0,0,0,114258.0,114258.0",
				hour, day);
		logger.info("Excpet has:{}",csv);
		assertTrue(output.contains(csv));
	}
	
	@Test
	public void testNoGroupAggr() throws Exception {
		String remoteCsvFolder = "/etltest/dstransform/";
		String csvtransProp = "csvAggrNoGroup.properties";
		String[] csvFiles = new String[] {"data1.data"};
		//prepare data
		String dataFile = "maxA"; 
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+dataFile), new Path("/data/"+dataFile));
		List<String> output = super.sparkTestACmd(remoteCsvFolder, csvFiles, csvtransProp, DatasetSqlCmd.class, InputFormatType.Text);
		logger.info("Output is:"+output);
		
		// assertion
		assertTrue(output.size() > 0);
		String sampleOutput = output.get(0);
		String[] csvs = sampleOutput.split(",", -1);
		assertTrue("1.0".equals(csvs[0]));
	}
	
	@Test
	public void mergeIntoMultiple() throws Exception {
		String remoteCsvFolder = "/etltest/dstransform/";
		String csvtransProp = "mergeIntoMultiple.properties";
		String[] csvFiles = new String[] {"MyCore_.do", "MyCore1_.do"};
		//prepare schema
		String schemaFolder = "/etltest/aggr/cfg/"; //hardcoded in the properties
		String schemaFile = "multipleTableSchemas.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.sparkTestACmd(remoteCsvFolder, csvFiles, csvtransProp, DatasetSqlCmd.class, InputFormatType.Text);
		logger.info("Output is:\n"+String.join("\n", output));
		String dt = "2016-03-28T11:05:00+00:00";
		String dtformat = "yyyy-MM-dd'T'HH:mm:ssXXX";
		String hour = GroupFun.hour(dt, dtformat);
		String day = GroupFun.day(dt, dtformat);
		String csv=String.format("%s,%s,PT300S,QDSD0101vSGS-L-NK-20,lcp-1,QDSD0101vSGS-L-NK-20-VLR-00,0,0,0,114258.0,114258.0",
				hour, day);
		logger.info(String.format("expect contains: \n%s", csv));
		assertTrue(output.contains(csv));
	}
	
	@Test
	public void testCsvCount() throws Exception {
		String staticCfgName = "csvcount.properties";
		String inputFolder = "/test/csvcount/input/";
		String csvFileName1 = "sample1.csv";
		String csvFileName2 = "sample2.csv";
		
		String[] inputFiles = new String[]{csvFileName1,csvFileName2};
		
		//copy csv file
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName1), new Path(inputFolder + csvFileName1));
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName2), new Path(inputFolder + csvFileName2));
		
		List<String> output = super.sparkTestACmd(inputFolder, inputFiles, staticCfgName, DatasetSqlCmd.class, InputFormatType.Text);
		logger.info("Output is:\n"+String.join("\n",output));
		
		Assert.assertEquals(5, output.size());
		Assert.assertTrue(output.contains("Name1,A,2"));
		Assert.assertTrue(output.contains("Name1,B,1"));
		Assert.assertTrue(output.contains("Name1,E,1"));
		Assert.assertTrue(output.contains("Name2,B,2"));
		Assert.assertTrue(output.contains("Name2,D,2"));
	}
}
