package etl.cmd.test;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.util.CombineWithFileNameTextInputFormat;
import etl.util.XmlInputFormat;
import scala.Tuple2;

public class TestCsvTransformCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvTransformCmd.class);
	public static final String testCmdClass="etl.cmd.CsvTransformCmd";

	public String getResourceSubFolder(){
		return "csvtrans/";
	}
	
	@Test
	public void testFileNameRowValidationSkipHeaderFun() throws Exception {
		String remoteCsvFolder = "/etltest/csvtransform/";
		String remoteCsvOutputFolder = "/etltest/csvtransformout/";
		String csvtransProp = "csvtrans.properties";
		String[] csvFiles = new String[] {"PJ24002A_BBG2.csv"};
		
		List<String> output = super.mapTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n" + String.join("\n", output));
		
		// assertion
		assertTrue(output.size() > 0);
		String sampleOutput = output.get(0);
		String[] csvs = sampleOutput.split(",", -1);
		int mergedColumn = 2;
		assertTrue("BBG2".equals(csvs[csvs.length - 1])); // check filename appended to last
		assertFalse("MeasTime".equals(csvs[0]));//skip header check
		assertTrue(csvs[mergedColumn].contains("-"));//check column merged
	}
	
	@Test
	public void testSplitCol() throws Exception {
		String remoteCsvFolder = "/etltest/csvtransform/";
		String remoteCsvOutputFolder = "/etltest/csvtransformout/";
		String csvtransProp = "csvtrans3.properties";
		String csvFile = "csvtrans2.csv";
		
		List<String> output = super.mapTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, new String[]{csvFile}, 
				testCmdClass, false);
		logger.info("Output is:"+output);
		assertTrue(output.size() > 0);
		String sampleOutput = output.get(0);
		String[] csvs = sampleOutput.split(",", -1);
		int splitColumn = 2;
		logger.info("Updated Column value"+csvs[splitColumn]+" "+ csvs[splitColumn+1] + " "+csvs[splitColumn+2]);
		assertFalse(csvs[splitColumn].contains("."));
	}
	
	@Test
	public void testUpdateCol() throws Exception {
		String remoteCsvFolder = "/etltest/csvtransform/";
		String remoteCsvOutputFolder = "/etltest/csvtransformout/";
		String csvtransProp = "csvtrans2.properties";
		String csvFile = "csvtrans2.csv";
		
		List<String> output = super.mapTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, new String[]{csvFile}, 
				testCmdClass, false);
		logger.info("Output is:"+output);
		assertTrue(output.size() > 0);
		String sampleOutput = output.get(0);
		String[] csvs = sampleOutput.split(",", -1);
		int updateColumn1 = 2;
		int updateColumn2 = 3;
		String replaceString1 = ".";
		String replaceString2 = "coarse";
		assertFalse(csvs[updateColumn1].contains(replaceString1.trim())); 
		assertFalse(csvs[updateColumn2].contains(replaceString2.trim()));
	}
	
	@Test
	public void testJsCallJava() throws Exception {
		String remoteCsvFolder = "/etltest/csvtransform/";
		String remoteCsvOutputFolder = "/etltest/csvtransformout/";
		String csvtransProp = "csvtrans.telecom.properties";
		String csvFile = "telecom.csv";
		
		List<String> output = super.mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, new String[]{csvFile}, 
				testCmdClass, false);
		logger.info("Output is:\n"+String.join("\n", output));
		String row1 = output.get(0);
		String[] row1fields = row1.split(",",-1);
		assertTrue(row1fields.length==6);
		String row4 = output.get(3);
		String[] row4fields = row4.split(",",-1);
		assertTrue("1".equals(row4fields[4]));
	}
	
	@Test
	public void testDynTrans() throws Exception {
		String remoteCsvFolder = "/etltest/csvdyntrans/input/";
		String remoteCsvOutputFolder = "/etltest/csvdyntrans/output/";
		String csvtransProp = "csvtransdyn1.properties";
		String csvFile = "csvtransdyn1_data.csv";
		
		//prepare schema
		String schemaFolder = "/etltest/csvdyntrans/schema/"; //hardcoded in the properties
		String schemaFile = "dynschema_test1_schemas.txt";
		getFs().copyFromLocalFile(false, true, new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
		
		List<String> output = super.mapTest(remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, new String[]{csvFile}, 
				testCmdClass, false);
		logger.info("Output is:"+output);
		String row1 = output.get(0);
		String[] row1fields = row1.split(",",-1);
		assertTrue(row1fields.length==9);
	}
	
	@Test
	public void transformMultipleFiles() throws Exception {
		String remoteCsvInputFolder = "/etltest/csvtrans/input/";
		String remoteCsvOutputFolder = "/etltest/csvtrans/output/";
		String csvtransProp = "csvtrans.multiplefiles.properties";
		String[] csvFiles = new String[]{"DPC_PoolType_nss7_","PoolType_mi_SNEType_"};
		
		List<String> output = super.mrTest(remoteCsvInputFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:"+output);
		//assert
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		List<String> outputs = HdfsUtil.listDfsFile(super.getFs(), remoteCsvOutputFolder);
		for (String csvFile: csvFiles){
			String outputCsv = csvFile + "-r-00000";
			assertTrue(outputs.contains(outputCsv));
			List<String> contents = HdfsUtil.stringsFromDfsFile(super.getFs(), remoteCsvOutputFolder+outputCsv);
			String content = contents.get(0);
			String[] fields = content.split(",", -1);
			Date d = sdf.parse(fields[0]);
			logger.info(String.format("date:%s", d));
		}
	}
	
	@Test
	public void emptyEndings() throws Exception {
		String remoteCsvInputFolder = "/etltest/csvtrans/input/";
		String remoteCsvOutputFolder = "/etltest/csvtrans/output/";
		String csvtransProp = "csvtrans.multiplefiles.properties";
		String[] csvFiles = new String[]{"MME_PoolType_vlr_"};
		
		List<String> output = super.mrTest(remoteCsvInputFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:"+output);
		//assert
		List<String> input = HdfsUtil.stringsFromDfsFile(super.getFs(), remoteCsvInputFolder+csvFiles[0]);
		int fn = 0;
		for (String in:input){
			fn = in.split(",", -1).length;
			logger.info(fn);
		}
		List<String> outputs = HdfsUtil.listDfsFile(super.getFs(), remoteCsvOutputFolder);
		for (String csvFile: csvFiles){
			String outputCsv = csvFile + "-r-00000";
			assertTrue(outputs.contains(outputCsv));
			List<String> contents = HdfsUtil.stringsFromDfsFile(super.getFs(), remoteCsvOutputFolder+outputCsv);
			for (String content:contents){
				String[] fields = content.split(",", -1);
				assertTrue(fn==fields.length);
			}
		}
	}
	
	@Test
	public void transformToSingleFile() throws Exception {
		String remoteCsvInputFolder = "/etltest/csvtrans/input/";
		String remoteCsvOutputFolder = "/etltest/csvtrans/output/";
		String csvtransProp = "csvtrans.tosinglefile.properties";
		String[] csvFiles = new String[]{"DPC_PoolType_nss7_","PoolType_mi_SNEType_"};
		
		List<String> output = super.mrTest(remoteCsvInputFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:"+output);
		//assert
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		List<String> outputs = HdfsUtil.listDfsFile(super.getFs(), remoteCsvOutputFolder);
		logger.info(outputs);
		assertTrue(outputs.contains("part-r-00000"));
	}
	
	@Test
	public void testCombineFileInputFormat() throws Exception{
		//
		String inputFolder = "/test/cfif/input/";
		String outputFolder = "/test/cfif/output/";
		
		String staticCfgName = "csvtrans.cfif.properties";
		String[] inputFiles = new String[]{
				"A20160409.0000-20160409.0100_E_000000000_262216704_F4D9FB75EA47_13.csv", 
				"A20160409.0000-20160409.0100_E_000000000_262216706_F4D9FB75EB2F_13.csv",
				"A20160409.0000-20160409.0100_E_000000000_262216708_F4D9FB766561_13.csv"};
		
		//run cmd
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(inputFolder, inputFiles));
		List<String> outputs = super.mrTest(rfifs, outputFolder, staticCfgName, testCmdClass, CombineWithFileNameTextInputFormat.class);
		logger.info(String.format("output:%s\n", String.join("\n", outputs)));
	}
}
