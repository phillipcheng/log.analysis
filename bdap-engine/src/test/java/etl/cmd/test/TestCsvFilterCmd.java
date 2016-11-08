package etl.cmd.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.CsvFilterCmd;

public class TestCsvFilterCmd extends TestETLCmd{
	
	public static final Logger logger = LogManager.getLogger(TestCsvFilterCmd.class);
	public static final String testCmdClass="etl.cmd.CsvFilterCmd";

	public String getResourceSubFolder(){
		return "csvfilter/";
	}
	
	@Test
	public void splitByExpMR() throws Exception {
		String remoteCsvFolder = "/etltest/filterinput/";
		String remoteCsvOutputFolder = "/etltest/filteroutput/";
		String csvsplitProp = "filterByExp.properties";
		String[] csvFiles = new String[] {"data1.data"};
		
		List<String> output = mrTest(remoteCsvFolder, remoteCsvOutputFolder, csvsplitProp, csvFiles, testCmdClass, false);
		logger.info(String.format("Output is: \n%s", String.join("\n", output)));
		
		// assertion
		assertTrue(output.size() > 0);
		
		List<String> files = HdfsUtil.listDfsFile(getFs(), remoteCsvOutputFolder);
		assertTrue(files.size()==4);
		logger.info("Output files: {}", files);
	}
	
	@Test
	public void splitByExpSpark() throws Exception {
		String csvSplitProp = "filterByExp.properties";
		String csvFile = "data1.data";
		
		String wfName="wfName1";
		String wfid = "wfid1";
		SparkConf conf = new SparkConf().setAppName(wfName).setMaster("local[3]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> ds = jsc.textFile(super.getLocalFolder()+csvFile);
		CsvFilterCmd cmd= new CsvFilterCmd(wfName, wfid, super.getLocalFolder()+csvSplitProp, super.getDefaultFS(), null);
		Map<String, JavaRDD<String>> ret = cmd.sparkSplitProcess(ds);
		String key = "csv";
		String csvString="csv, abcd, eff";
		String sesString="ses, dbbd, qqq";
		JavaRDD<String> csvrdd = ret.get(key);
		assertTrue(csvrdd.collect().contains(csvString));
		assertTrue(!csvrdd.collect().contains(sesString));
		key="ses";
		JavaRDD<String> sesrdd = ret.get(key);
		assertTrue(sesrdd.collect().contains(sesString));
		assertTrue(!sesrdd.collect().contains(csvString));
		
		jsc.close();
		// assertion
		
	}

}
