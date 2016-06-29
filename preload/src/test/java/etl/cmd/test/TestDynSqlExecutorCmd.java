package etl.cmd.test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.cmd.dynschema.DynSqlExecutorCmd;
import etl.util.DBUtil;
import etl.util.Util;

import org.apache.log4j.Logger;

public class TestDynSqlExecutorCmd extends TestETLCmd{

	public static final Logger logger = Logger.getLogger(TestDynSqlExecutorCmd.class);

	@Test
	public void test1() throws Exception{
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
		try {
			//
			String inputFolder = "/test/dynsqlexecutor/input/";
			String dfsCfgFolder = "/test/dynsqlexecutor/cfg/";
			String schemaFolder="/test/dynsqlexecutor/schema/";
					
			String staticCfgName = "dynsqlexecutor1.properties";
			String wfid="wfid1";
			String prefix = "sgsiwf";
			String dynInCfgName = "dynschema_test1_dyncfgout_wfid1";
			String localSchemaFileName = "dynschema_test1_schemas.txt";
			String localCsvFileName = "dynschema_test1_wfid1_MyCore_.csv";
			
			String remoteCsvFileName = wfid + "_" + "MyCore_" + ".csv";
			
			//generate all the data files
			getFs().delete(new Path(inputFolder), true);
			getFs().delete(new Path(dfsCfgFolder), true);
			getFs().delete(new Path(schemaFolder), true);
			//
			getFs().mkdirs(new Path(inputFolder));
			getFs().mkdirs(new Path(dfsCfgFolder));
			getFs().mkdirs(new Path(schemaFolder));

			//copy static cfg
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsCfgFolder + staticCfgName));
			//copy dynamic cfg
			getFs().copyFromLocalFile(new Path(getLocalFolder() + dynInCfgName), new Path(dfsCfgFolder + dynInCfgName));
			//copy schema file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + prefix +"." + DynSchemaCmd.schema_name));
			//copy csv file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localCsvFileName), new Path(inputFolder + remoteCsvFileName));
			
			//run cmd
			DynSqlExecutorCmd cmd = new DynSqlExecutorCmd(wfid, dfsCfgFolder + staticCfgName, dfsCfgFolder+dynInCfgName, null, getDefaultFS());
			cmd.process(0, null, null);
		
			//check results
			
			//schemaFolder should have the schema file
			List<String> files = Util.listDfsFile(getFs(), schemaFolder);
			String schemaFileName = String.format("%s.schemas.txt", prefix);
			assertTrue(files.size()==1);
			assertTrue(files.contains(schemaFileName));
					
			List<String> files1 = Util.listDfsFile(getFs(), inputFolder);
			String csvFileName = String.format("%s_%s.csv", wfid, "MyCore_");
			assertTrue(files1.size()==1);
			assertTrue(files1.contains(csvFileName));
		
		
			//checking create table already created
			String sql = "SELECT * from sgsiwf.MyCore_;";
			PropertiesConfiguration pc = Util.getPropertiesConfigFromDfs(getFs(), dfsCfgFolder + staticCfgName);
			int result=DBUtil.checkSqls(sql,pc);
			logger.info("The result is file "+result);
			assertTrue(result==1);
			
			//csvData
			String line;
			BufferedReader br = new BufferedReader(new FileReader(getLocalFolder() + localCsvFileName));
			ArrayList<String> csvData=new ArrayList<String>();
			while ((line = br.readLine()) != null) {
			    String colValue = line;
			    logger.info(colValue);
			    csvData.add(colValue);  
			}
			logger.info("The CSV File data is: ");
			for (String csvdata : csvData)
			{
				logger.info(csvdata);
			}
			//table data
			ArrayList<String> cols=new ArrayList<String>() ;
			cols=DBUtil.checkCsv(sql, pc);
			logger.info("The Table data is: ");
			for (String tbdata : cols)
			{
				logger.info(tbdata);
			}
			assertTrue(cols.containsAll(csvData));
		} catch (Exception e) {
			logger.error("Exception occured due to invalid data-history path", e);
		}
		return null;
			}
		});
		
	}
	
	@Test
	public void remoteTest1() throws Exception{
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				test1();
				return null;
			}
		});
		
	}
}