package etl.cmd.test;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class TestCsvCountCmd extends TestETLCmd{
	
	private static final String cmdClassName = "etl.cmd.CsvCountCmd";
	
	public static final Logger logger = LogManager.getLogger(TestCsvCountCmd.class);

//	@Test
	public void testSample() {
//		String exp="[\"1\",\"2\"]";
//		String exp="var StringArray=Java.type(\"java.lang.String[]\");var val=new StringArray(2); val[0]=\"a\", val[1]=\"b\";val";
		String exp="var util=Java.type(\"etl.cmd.test.TestCsvCountCmd\");util.getValues()";
		String[] value = (String[])ScriptEngineUtil.eval(exp, VarType.OBJECT, null);
		
		logger.info("Value is:{}",value.getClass());
		for(String str:value){
			logger.info("{}",str);
		}		
	}
	
	@Test
	public void testmr_groupdim() {
		String staticCfgName = "csvcount.conf.properties";
		String wfid="wfid1";
		String dfsCfgFolder = "/test/csvcount/cfg/";
		String inputFolder = "/test/csvcount/input/";
		String outputFolder = "/test/csvcount/output/";
		String csvFileName1 = "sample1.csv";
		String csvFileName2 = "sample2.csv";
		
		String[] inputFiles = new String[]{csvFileName1,csvFileName2};
		
		try {
			
			getFs().delete(new Path(dfsCfgFolder), true);
			getFs().delete(new Path(inputFolder), true);
			getFs().delete(new Path(outputFolder), true);
			
			getFs().mkdirs(new Path(dfsCfgFolder));
			getFs().mkdirs(new Path(inputFolder));
			getFs().mkdirs(new Path(outputFolder));
			
			//copy static cfg
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsCfgFolder + staticCfgName));			
			
			//copy csv file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName1), new Path(inputFolder + csvFileName1));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName2), new Path(inputFolder + csvFileName2));
			getFs().setPermission(new Path(inputFolder), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			
			getFs().setPermission(new Path(inputFolder + csvFileName1), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			getFs().setPermission(new Path(inputFolder + csvFileName2), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));	
			
			List<String> output = super.mrTest(inputFolder, outputFolder, staticCfgName, inputFiles,cmdClassName, false);
			logger.info("Output is:\n"+String.join("\n",output));
			
			Assert.assertEquals(5, output.size());
			Assert.assertTrue(output.contains("Name1,A,2"));
			Assert.assertTrue(output.contains("Name1,B,1"));
			Assert.assertTrue(output.contains("Name1,E,1"));
			Assert.assertTrue(output.contains("Name2,B,2"));
			Assert.assertTrue(output.contains("Name2,D,2"));
			
		}  catch (Exception e) {
			logger.error("Exception occured due to invalid data-history path", e);
		}
	}
	
	@Test
	public void testmr_splitdim() {
		String staticCfgName = "csvcount.conf_split.properties";
		String wfid="wfid1";
		String dfsCfgFolder = "/test/csvcount/cfg/";
		String inputFolder = "/test/csvcount/input/";
		String outputFolder = "/test/csvcount/output/";
		String csvFileName1 = "sample3.csv";
		String csvFileName2 = "sample4.csv";
		
		String[] inputFiles = new String[]{csvFileName1,csvFileName2};
		
		try {
			
			getFs().delete(new Path(dfsCfgFolder), true);
			getFs().delete(new Path(inputFolder), true);
			getFs().delete(new Path(outputFolder), true);
			
			getFs().mkdirs(new Path(dfsCfgFolder));
			getFs().mkdirs(new Path(inputFolder));
			getFs().mkdirs(new Path(outputFolder));
			
			//copy static cfg
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsCfgFolder + staticCfgName));			
			
			//copy csv file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName1), new Path(inputFolder + csvFileName1));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName2), new Path(inputFolder + csvFileName2));
			getFs().setPermission(new Path(inputFolder), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			
			getFs().setPermission(new Path(inputFolder + csvFileName1), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			getFs().setPermission(new Path(inputFolder + csvFileName2), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));	
			
			List<String> output = super.mrTest(inputFolder, outputFolder, staticCfgName, inputFiles,cmdClassName, false);
			logger.info("Output is:\n"+String.join("\n",output));
			
			Assert.assertEquals(5, output.size());
			Assert.assertEquals("2016-10-12 10:10:00.0,2016-10-12 10:10:04.999,1", output.get(0));
			Assert.assertEquals("2016-10-12 10:10:05.0,2016-10-12 10:10:09.999,1", output.get(1));
			Assert.assertEquals("2016-10-12 10:10:10.0,2016-10-12 10:10:14.999,1", output.get(2));
			Assert.assertEquals("2016-10-12 10:10:15.0,2016-10-12 10:10:19.999,2", output.get(3));
			Assert.assertEquals("2016-10-12 10:10:20.0,2016-10-12 10:10:24.999,1", output.get(4));			
		}  catch (Exception e) {
			logger.error("Exception occured due to invalid data-history path", e);
		}
	}

	@Override
	public String getResourceSubFolder() {
		return "csvcount/";
	}
}
