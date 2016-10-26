package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.CsvFileGenCmd;
import etl.util.Util;


public class TestCsvGenCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvGenCmd.class);
	public static final String testCmdClass = "etl.cmd.CsvFileGenCmd";

	public String getResourceSubFolder(){
		return "csvgen/";
	}
	
	private void test1Fun() throws Exception {
		try {
			//
			String remoteCfgFolder = "/test/csvgen/cfg/";
			String remoteSchemaFolder = "/test/csvgen/schema/";
			String staticCfg = "csvgen1.properties";
			String schemaFile = "schema1.txt";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			
			getFs().delete(new Path(remoteSchemaFolder), true);
			getFs().mkdirs(new Path(remoteSchemaFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteSchemaFolder + schemaFile));
			
			CsvFileGenCmd cmd = new CsvFileGenCmd("wf1", "wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			
			getFs().delete(new Path(cmd.getOutputFolder()), true);
			getFs().mkdirs(new Path(cmd.getOutputFolder()));
			
			List<String> ret = cmd.sgProcess();
			
			//assertion
			String outputFolder = cmd.getOutputFolder();
			int fileSize = cmd.getFileSize();
			List<String> files = HdfsUtil.listDfsFile(super.getFs(), outputFolder);
			assertTrue(files.size()>=1);
			String fileName = files.get(0);
			List<String> contents = HdfsUtil.stringsFromDfsFile(super.getFs(), outputFolder + fileName);
			assertTrue(contents.size()==fileSize);
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void test1() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			test1Fun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					test1Fun();
					return null;
				}
			});
		}
	}
}
