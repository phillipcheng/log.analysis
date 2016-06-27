package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.cmd.dynschema.DynSqlExecutorCmd;
import etl.util.Util;

import org.apache.log4j.Logger;

public class TestDynSqlExecutorCmd extends TestETLCmd{

	public static final Logger logger = Logger.getLogger(TestDynSqlExecutorCmd.class);

	@Test
	public void test1(){
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
			//check db
			
		} catch (Exception e) {
			logger.error("Exception occured due to invalid data-history path", e);
		}
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