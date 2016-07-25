package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.util.Util;

import org.apache.log4j.Logger;

public class TestDynSchemaCmd extends TestETLCmd{

	public static final Logger logger = Logger.getLogger(TestDynSchemaCmd.class);

	private void test1Fun() throws Exception{
			try {
			//
			String inputFolder = "/test/dynschemacmd/input/";
			String outputFolder = "/test/dynschemacmd/output/";
			String dfsCfgFolder = "/test/dynschemacmd/cfg/";
			String schemaFolder="/test/dynschemacmd/schema/";
			String schemaHistoryFolder="/test/dynschemacmd/schemahistory/";
			
			String staticCfgName = "dynschema_test1.properties";
			String wfid="wfid1";
			String dynCfgOutName ="dynschema_test1_dyncfgout_" + wfid;
			String prefix = "sgsiwf";
			
			String[] inputFiles = new String[]{"dynschema_test1_data.xml"};
			
			String localFile = "dynschema_test1_data.xml";
			
			//generate all the data files
			getFs().delete(new Path(inputFolder), true);
			getFs().delete(new Path(outputFolder), true);
			getFs().delete(new Path(dfsCfgFolder), true);
			getFs().delete(new Path(schemaFolder), true);
			getFs().delete(new Path(schemaHistoryFolder), true);
			//
			getFs().mkdirs(new Path(inputFolder));
			getFs().mkdirs(new Path(outputFolder));
			getFs().mkdirs(new Path(dfsCfgFolder));
			getFs().mkdirs(new Path(schemaFolder));
			getFs().mkdirs(new Path(schemaHistoryFolder));
			
			for (String inputFile: inputFiles){
				getFs().copyFromLocalFile(new Path(getLocalFolder() + localFile), new Path(inputFolder + inputFile));
			}
			//add the local conf file to dfs
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsCfgFolder + staticCfgName));
			
			//run cmd
			DynSchemaCmd cmd = new DynSchemaCmd(wfid, dfsCfgFolder + staticCfgName, dfsCfgFolder+dynCfgOutName, getDefaultFS(), null);
			cmd.sgProcess();
			
			//check results
			//outputFolder should have the csv file
			List<String> files = Util.listDfsFile(getFs(), outputFolder);
			String csvFileName = String.format("%s_%s.csv", wfid, "MyCore_");
			assertTrue(files.size()==1);
			assertTrue(files.contains(csvFileName));
			
			//schemaFolder should have the schema file
			files = Util.listDfsFile(getFs(), schemaFolder);
			String schemaFileName = "schemas.txt";
			assertTrue(files.size()==1);
			assertTrue(files.contains(schemaFileName));
			
			//schemaHistoryFolder should have the sql files
			files = Util.listDfsFile(getFs(), schemaHistoryFolder);
			String[] sqlFiles = new String[]{"copys","createtables","droptables","trunctables"};
			assertTrue(files.size()==sqlFiles.length);
			for (String sqlFile:sqlFiles){
				String sqlFn = String.format("%s.%s.sql_%s", prefix, sqlFile, wfid);
				assertTrue(files.contains(sqlFn));
			}
			//dfsCfgFolder should have the dyn out cfg file
			files = Util.listDfsFile(getFs(), dfsCfgFolder);
			assertTrue(files.contains(dynCfgOutName));
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