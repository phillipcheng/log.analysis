package etl.cmd.test;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.FileNameUpdateCmd;


public class TestFileNameUpdateCmd extends TestETLCmd{
	
	private static final String cmdClassName = "etl.cmd.FileNameUpdateCmd";

	//@Test
	public void testsgProcess() {
		String staticCfgName = "conf.properties";
		String wfid="wfid1";
		String dfsCfgFolder = "/test/filenameupdate/cfg/";
		String inputFolder = "/test/filenameupdate/input/";
		String csvFileName1 = "BBTPNJ33-FDB-01-2_eFemto_MAPPING_FILE_SAMSUNG.csv";
		String csvFileName2 = "BBTPNJ33-FDB-01-3_eFemto_MAPPING_FILE_SAMSUNG.csv";
		
		try {
			
			getFs().delete(new Path(dfsCfgFolder), true);
			getFs().delete(new Path(inputFolder), true);
			
			getFs().mkdirs(new Path(dfsCfgFolder));
			getFs().mkdirs(new Path(inputFolder));
			
			//copy static cfg
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsCfgFolder + staticCfgName));			
			
			//copy csv file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName1), new Path(inputFolder + csvFileName1));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName2), new Path(inputFolder + csvFileName2));
			getFs().setPermission(new Path(inputFolder), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			
			getFs().setPermission(new Path(inputFolder + csvFileName1), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			getFs().setPermission(new Path(inputFolder + csvFileName2), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			
			
			FileNameUpdateCmd fnucmd=new FileNameUpdateCmd("wf1", wfid, dfsCfgFolder + staticCfgName, getDefaultFS(), null);			
			fnucmd.sgProcess();
			
			//result
			List<String> output = HdfsUtil.listDfsFile(getFs(), inputFolder);
			logger.info("Output Result:{}", output);
			Assert.assertTrue("Contain BBTPNJ33-FDB-01-2_20160412_eFemto_MAPPING_FILE_SAMSUNG.csv",output.contains("BBTPNJ33-FDB-01-2_20160412_eFemto_MAPPING_FILE_SAMSUNG.csv"));
			Assert.assertTrue("Contain BBTPNJ33-FDB-01-3_20160909_eFemto_MAPPING_FILE_SAMSUNG.csv",output.contains("BBTPNJ33-FDB-01-3_20160909_eFemto_MAPPING_FILE_SAMSUNG.csv"));
			Assert.assertEquals(2, output.size());
			
		}  catch (Exception e) {
			logger.error("Exception occured due to invalid data-history path", e);
		}		
	}
	
	@Test
	public void testmr() {
		String staticCfgName = "conf.properties";
		String wfid="wfid1";
		String dfsCfgFolder = "/test/filenameupdate/cfg/";
		String inputFolder = "/test/filenameupdate/input/";
		String outputFolder = "/test/filenameupdate/output/";
		String csvFileName1 = "BBTPNJ33-FDB-01-2_eFemto_MAPPING_FILE_SAMSUNG.csv";
		String csvFileName2 = "BBTPNJ33-FDB-01-3_eFemto_MAPPING_FILE_SAMSUNG.csv";
		
		String[] inputFiles = new String[]{csvFileName1,csvFileName2};
		
		logger.info("HADOOP_HOME:{}",System.getenv("HADOOP_HOME"));
		
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
			
			super.mrTest(dfsCfgFolder, inputFolder, outputFolder, staticCfgName, inputFiles,cmdClassName, true);
			
			//result
			List<String> output = HdfsUtil.listDfsFile(getFs(), inputFolder);
			logger.info("Output Result:{}", output);
			Assert.assertTrue("Contain BBTPNJ33-FDB-01-2_20160412_eFemto_MAPPING_FILE_SAMSUNG.csv",output.contains("BBTPNJ33-FDB-01-2_20160412_eFemto_MAPPING_FILE_SAMSUNG.csv"));
			Assert.assertTrue("Contain BBTPNJ33-FDB-01-3_20160909_eFemto_MAPPING_FILE_SAMSUNG.csv",output.contains("BBTPNJ33-FDB-01-3_20160909_eFemto_MAPPING_FILE_SAMSUNG.csv"));
			Assert.assertEquals(2, output.size());
			
		}  catch (Exception e) {
			logger.error("Exception occured due to invalid data-history path", e);
		}		
	}

	@Override
	public String getResourceSubFolder() {
		return "filenameupdate/";
	}

}
