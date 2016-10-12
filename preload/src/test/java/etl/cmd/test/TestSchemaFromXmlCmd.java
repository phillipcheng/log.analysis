package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import etl.cmd.XmlToCsvCmd;
import etl.util.HdfsUtil;
import etl.util.Util;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestSchemaFromXmlCmd extends TestETLCmd{

	public static final Logger logger = LogManager.getLogger(TestSchemaFromXmlCmd.class);

	public String getResourceSubFolder(){
		return "xmltocsv/";
	}
	
	private void test1Fun() throws Exception{
		try {
			//
			String inputFolder = "/test/dynschemacmd/input/";
			String dfsCfgFolder = "/test/dynschemacmd/cfg/";
			String schemaFolder="/test/dynschemacmd/schema/";
			String schemaHistoryFolder="/test/dynschemacmd/schemahistory/";
			String sqlFileName = "createtables.sql_wfid1";
			
			String staticCfgName = "dynschema_test1.properties";
			String wfid="wfid1";
			String prefix = "sgsiwf";
			
			String[] inputFiles = new String[]{"dynschema_test1_data.xml"};
			
			String localFile = "dynschema_test1_data.xml";
			
			//generate all the data files
			getFs().delete(new Path(inputFolder), true);
			getFs().delete(new Path(dfsCfgFolder), true);
			getFs().delete(new Path(schemaFolder), true);
			getFs().delete(new Path(schemaHistoryFolder), true);
			//
			getFs().mkdirs(new Path(inputFolder));
			getFs().mkdirs(new Path(dfsCfgFolder));
			getFs().mkdirs(new Path(schemaFolder));
			getFs().mkdirs(new Path(schemaHistoryFolder));
			
			for (String inputFile: inputFiles){
				getFs().copyFromLocalFile(new Path(getLocalFolder() + localFile), new Path(inputFolder + inputFile));
			}
			//add the local conf file to dfs
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsCfgFolder + staticCfgName));
			
			//run cmd
			XmlToCsvCmd cmd = new XmlToCsvCmd("wf1", wfid, dfsCfgFolder + staticCfgName, getDefaultFS(), null);
			List<String> info = cmd.sgProcess();
			int numFiles = Integer.parseInt(info.get(0));
			logger.info(info);
			
			List<String> files = null;
			//schemaFolder should have the schema file
			files = HdfsUtil.listDfsFile(getFs(), schemaFolder);
			String schemaFileName = "schemas.txt";
			assertTrue(files.size()==1);
			assertTrue(files.contains(schemaFileName));
			
			//schemaHistoryFolder should have the sql files
			List<String> sqlContent = HdfsUtil.stringsFromDfsFile(getFs(), schemaHistoryFolder + sqlFileName);
			logger.info(sqlContent);
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