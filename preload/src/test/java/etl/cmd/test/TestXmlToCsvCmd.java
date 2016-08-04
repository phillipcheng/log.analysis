package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import etl.util.Util;

import org.apache.log4j.Logger;

public class TestXmlToCsvCmd extends TestETLCmd{

	public static final Logger logger = Logger.getLogger(TestXmlToCsvCmd.class);
	
	private static final String cmdClassName = "etl.cmd.dynschema.XmlToCsvCmd";

	public String getResourceSubFolder(){
		return "dynschema/";
	}
	
	private void test1Fun() throws Exception{
		try {
			//
			String inputFolder = "/test/xml2csv/input/";
			String outputFolder = "/test/xml2csv/output/";
			String dfsCfgFolder = "/test/xml2csv/cfg/";
			String schemaFolder="/test/xml2csv/schema/";
			
			String staticCfgName = "xml2csv1.properties";
			String[] inputFiles = new String[]{"dynschema_test1_data.xml"};
			String localSchemaFile = "dynschema_test1_schemas.txt";
			String remoteSchemaFile = "schemas.txt";
			
			//schema
			getFs().delete(new Path(schemaFolder), true);
			getFs().mkdirs(new Path(schemaFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFile), new Path(schemaFolder + remoteSchemaFile));
			
			//run cmd
			super.mrTest(dfsCfgFolder, inputFolder, outputFolder, staticCfgName, inputFiles, cmdClassName, true, true);
			
			//check results
			//outputFolder should have the csv file
			List<String> files = Util.listDfsFile(getFs(), outputFolder);
			logger.info(files);
			String csvFileName = String.format("%s-r-00000", "MyCore_");
			assertTrue(files.contains(csvFileName));
		} catch (Exception e) {
			logger.error("Exception occured ", e);
		}
	}
	
	@Test
	public void test1() throws Exception{
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