package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestCsvTransformCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestCsvTransformCmd.class);
	public static final String testCmdClass="etl.cmd.CsvTransformCmd";

	public String getResourceSubFolder(){
		return "csvtrans/";
	}
	
	private void testFileNameRowValidationSkipHeaderFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			String csvtransProp = "csvtrans.properties";
			String[] csvFiles = new String[] {"PJ24002A_BBG2.csv"};
			
			List<String> output = super.mapTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, testCmdClass, false);
			logger.info("Output is:"+output);
			
			// assertion
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			int mergedColumn = 2;
			logger.info("Last element:"+csvs[csvs.length - 1]+" "+ csvs[mergedColumn] + " "+csvs[mergedColumn-1]+ " " +csvs[mergedColumn+1]);
			assertTrue("BBG2".equals(csvs[csvs.length - 1])); // check filename appended to last
			assertFalse("MeasTime".equals(csvs[0]));//skip header check
			assertTrue(csvs[mergedColumn].contains("-"));//check column merged
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testFileNameRowValidationSkipHeader() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testFileNameRowValidationSkipHeaderFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testFileNameRowValidationSkipHeaderFun();
					return null;
				}
			});
		}
	}
	

	private void testSplitColFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			String csvtransProp = "csvtrans3.properties";
			String csvFile = "csvtrans2.csv";
			
			List<String> output = super.mapTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, new String[]{csvFile}, 
					testCmdClass, false);
			logger.info("Output is:"+output);
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			int splitColumn = 2;
			logger.info("Updated Column value"+csvs[splitColumn]+" "+ csvs[splitColumn+1] + " "+csvs[splitColumn+2]);
			assertFalse(csvs[splitColumn].contains("."));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	@Test
	public void testSplitCol() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testSplitColFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testSplitColFun();
					return null;
				}
			});
		}
	}
	
	private void testUpdateColFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			String csvtransProp = "csvtrans2.properties";
			String csvFile = "csvtrans2.csv";
			
			List<String> output = super.mapTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, new String[]{csvFile}, 
					testCmdClass, false);
			logger.info("Output is:"+output);
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			int updateColumn1 = 2;
			int updateColumn2 = 3;
			String replaceString1 = ".";
			String replaceString2 = "coarse";
			assertFalse(csvs[updateColumn1].contains(replaceString1.trim())); 
			assertFalse(csvs[updateColumn2].contains(replaceString2.trim()));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testUpdateCol() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testUpdateColFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testUpdateColFun();
					return null;
				}
			});
		}
	}
	
	private void testJsCallJavaFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			String csvtransProp = "csvtrans.telecom.properties";
			String csvFile = "telecom.csv";
			
			List<String> output = super.mapTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, new String[]{csvFile}, 
					testCmdClass, false);
			logger.info("Output is:"+output);
			String row1 = output.get(0);
			String[] row1fields = row1.split(",",-1);
			assertTrue(row1fields.length==6);
			String row4 = output.get(3);
			String[] row4fields = row4.split(",",-1);
			assertTrue("1".equals(row4fields[4]));
			assertTrue("240-449".equals(row4fields[5]));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testJsCallJava() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testJsCallJavaFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testJsCallJavaFun();
					return null;
				}
			});
		}
	}
	
	private void testDynTransFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/csvdyntrans/cfg/";
			String remoteCsvFolder = "/etltest/csvdyntrans/input/";
			String remoteCsvOutputFolder = "/etltest/csvdyntrans/output/";
			String csvtransProp = "csvtransdyn1.properties";
			String csvFile = "csvtransdyn1_data.csv";
			
			//prepare schema
			String schemaFolder = "/etltest/csvdyntrans/schema/"; //hardcoded in the properties
			String schemaFile = "dynschema_test1_schemas.txt";

			getFs().delete(new Path(schemaFolder), true);
			getFs().mkdirs(new Path(schemaFolder));
			getFs().copyFromLocalFile(new Path(this.getLocalFolder()+schemaFile), new Path(schemaFolder+schemaFile));
			
			List<String> output = super.mapTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, csvtransProp, new String[]{csvFile}, 
					testCmdClass, false);
			logger.info("Output is:"+output);
			String row1 = output.get(0);
			String[] row1fields = row1.split(",",-1);
			assertTrue(row1fields.length==9);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testDynTrans() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testDynTransFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testDynTransFun();
					return null;
				}
			});
		}
	}
	
	private void transformMultipleFiles() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/csvtrans/cfg/";
			String remoteCsvInputFolder = "/etltest/csvtrans/input/";
			String remoteCsvOutputFolder = "/etltest/csvtrans/output/";
			String csvtransProp = "csvtrans.multiplefiles.properties";
			String[] csvFiles = new String[]{"DPC_PoolType_nss7_-r-00000","PoolType_mi_SNEType_-r-00000"};
			
			List<String> output = super.mapTest(remoteCfgFolder, remoteCsvInputFolder, remoteCsvOutputFolder, csvtransProp, csvFiles, 
					testCmdClass, false);
			logger.info("Output is:"+output);
			//assert
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testTransformMultipleFiles() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			transformMultipleFiles();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					transformMultipleFiles();
					return null;
				}
			});
		}
	}
}
