package etl.cmd.test;

import static org.junit.Assert.assertTrue;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;
import etl.cmd.LoadDataCmd;
import etl.util.DBType;
import etl.util.DBUtil;

public class TestLoadDatabaseCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestLoadDatabaseCmd.class);
	
	public String getResourceSubFolder(){
		return "loadcsv/";
	}
	
	private void loadDynSchemaFun() throws Exception{
		try {
			String staticCfgName = "loadcsvds1.properties";
			String wfid="wfid1";
			String prefix = "sgsiwf";
			String localSchemaFileName = "test1_schemas.txt";
			String csvFileName = "MyCore_.csv";

			String inputFolder = "/test/loadcsv/input/";
			String dfsCfgFolder = "/test/loadcsv/cfg/";

			String schemaFolder="/test/loadcsv/schema/";
			
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
			//copy schema file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
			//copy csv file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(inputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
			//run cmd
			LoadDataCmd cmd = new LoadDataCmd(wfid, dfsCfgFolder + staticCfgName, getDefaultFS(), null);
			
			DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
			cmd.sgProcess();
			List<String> sqls = cmd.getCopysqls();
			DBUtil.executeSqls(cmd.getDropSqls(), cmd.getPc());
			String hdfsroot = cmd.getPc().getString("hdfs.webhdfs.root");
			String dbuser = cmd.getPc().getString("db.user");
			//assertion
			logger.info(sqls);
			String sqlVertica = "copy sgsiwf.MyCore_(endTime enclosed by '\"',duration enclosed by '\"',SubNetwork enclosed by '\"',"
					+ "ManagedElement enclosed by '\"',Machine enclosed by '\"',MyCore enclosed by '\"',UUID enclosed by '\"',"
					+ "VS_avePerCoreCpuUsage enclosed by '\"',VS_peakPerCoreCpuUsage enclosed by '\"') "
					+ String.format("SOURCE Hdfs(url='%s/test/loadcsv/input/MyCore_.csv*',username='%s') delimiter ',';", hdfsroot, dbuser);
			String sqlHive = String.format("load data inpath '%s/test/loadcsv/input/MyCore_.csv' into table sgsiwf.MyCore_", hdfsroot);
			logger.info("sqlVertica:" + sqlVertica);
			if (cmd.getDbtype()==DBType.HIVE){
				assertTrue(sqls.contains(sqlHive));
			}else{
				assertTrue(sqls.contains(sqlVertica));
			}
			
		} catch (Exception e) {
			logger.error("Exception occured due to invalid data-history path", e);
		}
	}

	@Test
	public void testLoadDynSchema() throws Exception{
		if (getDefaultFS().contains("127.0.0.1")){
			loadDynSchemaFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					loadDynSchemaFun();
					return null;
				}
			});
		}
	}
	
	private void loadDynSchemaExpFun() throws Exception{
		try {
			String staticCfgName = "loadcsvdsexp.properties";
			String wfid="wfid1";
			String prefix = "sgsiwf";
			String localSchemaFileName = "multipleTableSchemas.txt";
			String[] csvFileNames = new String[]{"MyCore_.csv", "MyCore1_.csv"};

			String inputFolder = "/test/loadcsv/input/";
			String dfsCfgFolder = "/test/loadcsv/cfg/";

			String schemaFolder="/test/loadcsv/schema/";
			
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
			//copy schema file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
			//copy csv file
			for (String csvFileName: csvFileNames){
				getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(inputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
			}
			//run cmd
			LoadDataCmd cmd = new LoadDataCmd(wfid, dfsCfgFolder + staticCfgName, getDefaultFS(), null);
			DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
			cmd.sgProcess();
			List<String> sqls = cmd.getCopysqls();
			DBUtil.executeSqls(cmd.getDropSqls(), cmd.getPc());

			String hdfsroot = cmd.getPc().getString("hdfs.webhdfs.root");
			String dbuser = cmd.getPc().getString("db.user");
			//assertion
			logger.info(sqls);
			String sqlVertica1 = "copy sgsiwf.MyCore_(endTime enclosed by '\"',duration enclosed by '\"',SubNetwork enclosed by '\"',"
					+ "ManagedElement enclosed by '\"',Machine enclosed by '\"',MyCore enclosed by '\"',UUID enclosed by '\"',"
					+ "VS_avePerCoreCpuUsage enclosed by '\"',VS_peakPerCoreCpuUsage enclosed by '\"') "
					+ String.format("SOURCE Hdfs(url='%s/test/loadcsv/input/MyCore_.csv*',username='%s') delimiter ',';", hdfsroot, dbuser);
			String sqlVertica2 = "copy sgsiwf.MyCore1_(endTime enclosed by '\"',duration enclosed by '\"',SubNetwork enclosed by '\"',"
					+ "ManagedElement enclosed by '\"',Machine enclosed by '\"',MyCore enclosed by '\"',UUID enclosed by '\"',"
					+ "VS_avePerCoreCpuUsage enclosed by '\"',VS_peakPerCoreCpuUsage enclosed by '\"') "
					+ String.format("SOURCE Hdfs(url='%s/test/loadcsv/input/MyCore1_.csv*',username='%s') delimiter ',';", hdfsroot, dbuser);
			String sqlHive1 = String.format("load data inpath '%s/test/loadcsv/input/MyCore_.csv' into table sgsiwf.MyCore_", hdfsroot);
			String sqlHive2 = String.format("load data inpath '%s/test/loadcsv/input/MyCore1_.csv' into table sgsiwf.MyCore1_", hdfsroot);
			
			if (DBType.HIVE == cmd.getDbtype()){
				assertTrue(sqls.contains(sqlHive1));
				assertTrue(sqls.contains(sqlHive2));
			}else{
				assertTrue(sqls.contains(sqlVertica1));
				assertTrue(sqls.contains(sqlVertica2));
			}
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	@Test
	public void testLoadDynSchemaExp() throws Exception{
		if (getDefaultFS().contains("127.0.0.1")){
			loadDynSchemaExpFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					loadDynSchemaExpFun();
					return null;
				}
			});
		}
	}
}
