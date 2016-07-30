package etl.cmd.test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import etl.cmd.dynschema.DynSqlExecutorCmd;
import etl.util.DBUtil;
import etl.util.Util;

import org.apache.log4j.Logger;

public class TestDynSqlExecutorCmd extends TestETLCmd{

	public static final Logger logger = Logger.getLogger(TestDynSqlExecutorCmd.class);

	public String getResourceSubFolder(){
		return "dynschema/";
	}
	
	@Before
    public void setUp() {
		setCfgProperties("testETLCmd_192.85.247.104.properties");
		super.setUp();
	}
	
	private void test1Fun() throws Exception{
		try {
			//
			
			String staticCfgName = "dynsqlexecutor1.properties";
			String wfid="wfid1";
			String prefix = "sgsiwf";
			String dynInCfgName = "dynschema_test1_dyncfgout_wfid1";
			String localSchemaFileName = "dynschema_test1_schemas.txt";
			String localCsvFileName = "dynschema_test1_wfid1_MyCore_.csv";

			String inputFolder = "/test/dynsqlexecutor/input/" + wfid + "/";
			String dfsCfgFolder = "/test/dynsqlexecutor/cfg/";
			String remoteCsvFileName = "MyCore_";

			String schemaFolder="/test/dynsqlexecutor/schema/";
			String schemaFileName = "schemas.txt";

			String remoteSqlFolder = "/test/dynschemacmd/schemahistory/";
			String sqlFile = "createtables.sql_wfid1";
			
			//generate all the data files
			getFs().delete(new Path(inputFolder), true);
			getFs().delete(new Path(dfsCfgFolder), true);
			getFs().delete(new Path(schemaFolder), true);
			getFs().delete(new Path(remoteSqlFolder), true);
			//
			getFs().mkdirs(new Path(inputFolder));
			getFs().mkdirs(new Path(dfsCfgFolder));
			getFs().mkdirs(new Path(schemaFolder));
			getFs().mkdirs(new Path(remoteSqlFolder));
			//copy static cfg
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsCfgFolder + staticCfgName));
			//copy dynamic cfg
			getFs().copyFromLocalFile(new Path(getLocalFolder() + dynInCfgName), new Path(dfsCfgFolder + dynInCfgName));
			//copy schema file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + schemaFileName));
			//copy csv file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localCsvFileName), new Path(inputFolder + remoteCsvFileName));//csv file must be csvfolder/wfid/tableName
			//copy sql
			getFs().copyFromLocalFile(new Path(getLocalFolder() + sqlFile), new Path(remoteSqlFolder + sqlFile));

			//run cmd
			DynSqlExecutorCmd cmd = new DynSqlExecutorCmd(wfid, dfsCfgFolder + staticCfgName, dfsCfgFolder+dynInCfgName, getDefaultFS(), null);
			cmd.sgProcess();

			//checking create table already created
			String sql ="SELECT table_name from tables where table_schema='"+prefix+"' and table_name='MyCore_';";
			PropertiesConfiguration pc = Util.getPropertiesConfigFromDfs(getFs(), dfsCfgFolder + staticCfgName);
			boolean result=DBUtil.checkTableExists(sql,pc);
			assertTrue(result);

			//get csvData to check
			ArrayList<String> csvData=new ArrayList<String>();
			String line,newline=null;
			int startIndex=1,endIndex=5;
			BufferedReader br=null;
			br = new BufferedReader(new FileReader(getLocalFolder() + localCsvFileName));
			while ((line = br.readLine()) != null) {
				String[] colArray = line.split("\",\"");
				newline="";
				for(int j=startIndex;j<endIndex;j++) {
					if(j==endIndex-1){
						newline=newline+colArray[j];
					}else{
						newline=newline+colArray[j]+" ";
					}
				}
				csvData.add(newline);  
			}
			br.close();
			// get table data
			ArrayList<String> cols=new ArrayList<String>() ;
			sql = "SELECT * from sgsiwf.MyCore_;";
			cols=DBUtil.checkCsv(sql, pc, startIndex+1, endIndex, " ");
			logger.info("The Comparation status :"+cols.containsAll(csvData));
			
			//check dbdata has csv data 
			assertTrue(cols.containsAll(csvData));
			logger.info("The results are verified successfully");
		} catch (Exception e) {
			logger.error("Exception occured due to invalid data-history path", e);
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