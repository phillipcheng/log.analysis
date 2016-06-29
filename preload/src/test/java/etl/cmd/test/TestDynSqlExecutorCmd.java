package etl.cmd.test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
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


					//checking create table already created
					String sql ="SELECT table_name from tables where table_schema='"+prefix+"' and table_name='MyCore_';";
					PropertiesConfiguration pc = Util.getPropertiesConfigFromDfs(getFs(), dfsCfgFolder + staticCfgName);
					int result=DBUtil.checkSqls(sql,pc);
					assertTrue(result==1);

					//get csvData to check
					ArrayList<String> csvData=new ArrayList<String>();
					String line,newline=null;
					BufferedReader br=null;
					br = new BufferedReader(new FileReader(getLocalFolder() + localCsvFileName));
					while ((line = br.readLine()) != null) {
						String[] colArray = line.split("\",\"");
						newline="";
						for(int j=1;j<colArray.length-2;j++)
						{
							newline=newline+colArray[j]+" ";
						}
						csvData.add(newline);  
					}

					// get table data
					ArrayList<String> cols=new ArrayList<String>() ;
					sql = "SELECT * from sgsiwf.MyCore_;";
					cols=DBUtil.checkCsv(sql, pc);

					//check dbdata has csv data 
					assertTrue(cols.containsAll(csvData));
					logger.info("The results are verified successfully");

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