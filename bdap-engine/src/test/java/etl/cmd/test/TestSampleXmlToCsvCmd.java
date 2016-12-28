package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import bdap.util.HdfsUtil;
import scala.Tuple2;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestSampleXmlToCsvCmd extends TestETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(TestSampleXmlToCsvCmd.class);
	
	private static final String cmdClassName = "etl.cmd.testcmd.SampleXml2CsvCmd";

	public String getResourceSubFolder(){
		return "xmltocsv/";
	}
	
	@Test
	public void test1() throws Exception{
		//
		String inputFolder = "/test/xml2csv/input/";
		String outputFolder = "/test/xml2csv/output/";
		String schemaFolder="/test/xml2csv/schema/";
		
		String staticCfgName = "xml2csv1.properties";
		String[] inputFiles = new String[]{"dynschema_test1_data.xml"};
		String localSchemaFile = "dynschema_test1_schemas.txt";
		String remoteSchemaFile = "schemas.txt";
		
		//schema
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFile), new Path(schemaFolder + remoteSchemaFile));
		
		//run cmd
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(inputFolder, inputFiles));
		getConf().set("xmlinput.start", "<measInfo>");
		getConf().set("xmlinput.end", "</measInfo>");
		getConf().set("xmlinput.row.start", "<measValue");
		getConf().set("xmlinput.row.end", "</measValue>");
		getConf().set("xmlinput.row.max.number", "3");
		super.mrTest(rfifs, outputFolder, staticCfgName, cmdClassName, etl.util.XmlInputFormat.class);
		
		//check results
		//outputFolder should have the csv file
		List<String> files = HdfsUtil.listDfsFile(getFs(), outputFolder);
		logger.info(files);
		String csvFileName = String.format("%s-r-00000", "MyCore_");
		assertTrue(files.contains(csvFileName));
		
		List<String> contents = HdfsUtil.stringsFromDfsFolder(getFs(), outputFolder);
		logger.info(String.format("contents:\n%s", String.join("\n", contents)));
		assertTrue(contents.size()==14);
		
		String firstLine = contents.get(0);
		String[] fields = firstLine.split(",", -1);
		assertTrue(fields.length==9);
	}
	
	@Test
	public void test1Spark() throws Exception{
		//
		String inputFolder = "/test/xml2csv/input/";
		String schemaFolder="/test/xml2csv/schema/";
		
		String staticCfgName = "xml2csv1.properties";
		String[] inputFiles = new String[]{"dynschema_test1_data.xml"};
		String localSchemaFile = "dynschema_test1_schemas.txt";
		String remoteSchemaFile = "schemas.txt";
		
		//schema
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFile), new Path(schemaFolder + remoteSchemaFile));
		
		Map<String, String> addConf = new HashMap<String, String>();
		addConf.put("xmlinput.start", "<measInfo>");
		addConf.put("xmlinput.end", "</measInfo>");
		addConf.put("xmlinput.row.start", "<measValue");
		addConf.put("xmlinput.row.end", "</measValue>");
		addConf.put("xmlinput.row.max.number", "3");
	
		Tuple2<List<String>, List<String>> ret = super.sparkTestKV(inputFolder, inputFiles, staticCfgName, 
				etl.cmd.testcmd.SampleXml2CsvCmd.class, etl.util.XmlInputFormat.class, addConf);
		List<String> output = ret._2;
		logger.info(String.format("output:\n%s", String.join("\n", output)));
		//check results
		assertTrue(output.size()==14);
		String firstLine = output.get(0);
		String[] fields = firstLine.split(",", -1);
		assertTrue(fields.length==9);
	}
	
	@Test
	public void testSchemaUpdate() throws Exception{
		//
		String inputFolder = "/test/dynschemacmd/input/";
		String outputFolder = "/test/dynschemacmd/output/";
		String schemaFolder="/test/dynschemacmd/schema/";
		String sqlFile = "/test/dynschemacmd/schemahistory/createtables.sql_wfid1";
		
		String staticCfgName = "xml2csv2.properties";
		String[] inputFiles = new String[]{"dynschema_test1_data2.xml"};
		
		//schema
		getFs().delete(new Path(schemaFolder + "schema-index.schema"), true);
		getFs().delete(new Path(schemaFolder + "MyCore_.schema"), true);
		getFs().delete(new Path(sqlFile), true);
		
		//run cmd
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(inputFolder, inputFiles));
		getConf().set("xmlinput.start", "<measInfo>");
		getConf().set("xmlinput.end", "</measInfo>");
		super.mrTest(rfifs, outputFolder, staticCfgName, cmdClassName, etl.util.XmlInputFormat.class);
		
		//check results
		//outputFolder should have the csv file
		List<String> files = HdfsUtil.listDfsFile(getFs(), outputFolder);
		logger.info(files);
		String csvFileName = String.format("%s-r-00000", "MyCore_");
		assertTrue(files.contains(csvFileName));
		
		List<String> sqlContents = HdfsUtil.stringsFromDfsFile(getFs(), sqlFile);
		logger.info(String.format("sqls:\n%s", String.join("\n", sqlContents)));
		assertTrue(sqlContents.size()==3);
		assertTrue(sqlContents.contains("alter table sgsiwf.MyCore_ add column VS_xPerCoreCpuUsage numeric(15,5)"));
		assertTrue(sqlContents.contains("alter table sgsiwf.MyCore_ add column VS_yPerCoreCpuUsage numeric(15,5)"));
	}
}