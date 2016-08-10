package etl.cmd.test;

import static org.junit.Assert.assertTrue;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;
import etl.cmd.LoadDataCmd;

public class TestLoadDatabaseCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestLoadDatabaseCmd.class);
	
	public String getResourceSubFolder(){
		return "loadcsv/";
	}
	
	private void test1Fun() throws Exception {
		
			//
			String dfsFolder = "/test/loaddata/cfg/";
			String staticCfgName = "loadcsv1.properties";
			String csvFolder = "/test/loaddata/csv/"; //specified in the properties
			String csvFileName = "part-loaddatabase";//specified in the properties

			getFs().delete(new Path(dfsFolder), true);
			getFs().delete(new Path(csvFolder), true);
			getFs().mkdirs(new Path(dfsFolder));
			getFs().mkdirs(new Path(csvFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsFolder + staticCfgName));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(csvFolder + csvFileName));

			// run cmd
			LoadDataCmd cmd = new LoadDataCmd("wfid1", dfsFolder + staticCfgName, getDefaultFS(),null);
			cmd.setExecute(false);
			List<String> sqls = cmd.sgProcess();
			
			//assertion
			logger.info(sqls);
			String sql = "copy lsl_sample (ProjectName, PackageName, TestCase, Row) "
					+ "SOURCE Hdfs(url='http://192.85.247.104:50070/webhdfs/v1/test/loaddata/csv/part-*',username='dbadmin') delimiter ','";
			assertTrue(sqls.contains(sql));
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
	
	private void expLoadFun() throws Exception {
		//
		String dfsFolder = "/test/loaddata/cfg/";
		String staticCfgName = "loadcsv2.properties";
		String csvFolder = "/test/loaddata/csv/";
		String csvFileName = "loadcsv2.monday";
		
		getFs().delete(new Path(dfsFolder), true);
		getFs().delete(new Path(csvFolder), true);
		getFs().mkdirs(new Path(dfsFolder));
		getFs().mkdirs(new Path(csvFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsFolder + staticCfgName));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(csvFolder + csvFileName));

		//run cmd
		LoadDataCmd cmd = new LoadDataCmd("wfid1", dfsFolder + staticCfgName, getDefaultFS(),null);
		cmd.setExecute(false);
		List<String> sqls =  cmd.sgProcess();
		
		//assertion
		logger.info(sqls);
		String sql = "copy lsl_sample (ProjectName, PackageName, TestCase, Row) "
				+ "SOURCE Hdfs(url='http://192.85.247.104:50070/webhdfs/v1/test/loaddata/csv/loadcsv2.monday',username='dbadmin') delimiter ','";
		assertTrue(sqls.contains(sql));
	}
	
	@Test
	public void testExpLoad() throws Exception{
		if (getDefaultFS().contains("127.0.0.1")){
			expLoadFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					expLoadFun();
					return null;
				}
			});
		}
	}
	
	private void multipleTables() throws Exception {
		//
		String dfsFolder = "/test/loaddata/cfg/";
		String staticCfgName = "loadcsv3.properties";
		String csvFolder = "/test/loaddata/csv/";
		String[] csvFileName = new String[]{"loadcsv2.monday","loadcsv3.monday"};
		
		getFs().delete(new Path(dfsFolder), true);
		getFs().delete(new Path(csvFolder), true);
		getFs().mkdirs(new Path(dfsFolder));
		getFs().mkdirs(new Path(csvFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsFolder + staticCfgName));
		for (String csv: csvFileName){
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csv), new Path(csvFolder + csv));
		}

		//run cmd
		LoadDataCmd cmd = new LoadDataCmd("wfid1", dfsFolder + staticCfgName, getDefaultFS(),null);
		cmd.setExecute(false);
		List<String> sqls =  cmd.sgProcess();
		
		//assertion
		logger.info(sqls);
		String sql1 = "copy lsl_sample (ProjectName, PackageName, TestCase, Row) "
				+ "SOURCE Hdfs(url='http://192.85.247.104:50070/webhdfs/v1/test/loaddata/csv/loadcsv2.monday',username='dbadmin') delimiter ','";
		String sql2 = "copy lsl_sample1 (ProjectName, PackageName, TestCase, Row) "
				+ "SOURCE Hdfs(url='http://192.85.247.104:50070/webhdfs/v1/test/loaddata/csv/loadcsv3.monday',username='dbadmin') delimiter ','";
		assertTrue(sqls.contains(sql1));
		assertTrue(sqls.contains(sql2));
	}
	
	@Test
	public void testmultipleTables() throws Exception{
		if (getDefaultFS().contains("127.0.0.1")){
			multipleTables();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					multipleTables();
					return null;
				}
			});
		}
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
			String schemaFileName = "schemas.txt";
			
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
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + schemaFileName));
			//copy csv file
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(inputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
			//run cmd
			LoadDataCmd cmd = new LoadDataCmd(wfid, dfsCfgFolder + staticCfgName, getDefaultFS(), null);
			cmd.setExecute(false);
			List<String> sqls = cmd.sgProcess();
			//assertion
			logger.info(sqls);
			String sql = "copy sgsiwf.MyCore_(endTime enclosed by '\"',duration enclosed by '\"',SubNetwork enclosed by '\"',"
					+ "ManagedElement enclosed by '\"',Machine enclosed by '\"',MyCore enclosed by '\"',UUID enclosed by '\"',"
					+ "VS_avePerCoreCpuUsage enclosed by '\"',VS_peakPerCoreCpuUsage enclosed by '\"') "
					+ "SOURCE Hdfs(url='http://192.85.247.104:50070/webhdfs/v1/test/loadcsv/input/MyCore_.csv*',username='dbadmin') delimiter ',';";
			assertTrue(sqls.contains(sql));
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
			String schemaFileName = "schemas.txt";
			
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
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + schemaFileName));
			//copy csv file
			for (String csvFileName: csvFileNames){
				getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(inputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
			}
			//run cmd
			LoadDataCmd cmd = new LoadDataCmd(wfid, dfsCfgFolder + staticCfgName, getDefaultFS(), null);
			cmd.setExecute(false);
			List<String> sqls = cmd.sgProcess();
			//assertion
			logger.info(sqls);
			String sql1 = "copy sgsiwf.MyCore_(endTime enclosed by '\"',duration enclosed by '\"',SubNetwork enclosed by '\"',"
					+ "ManagedElement enclosed by '\"',Machine enclosed by '\"',MyCore enclosed by '\"',UUID enclosed by '\"',"
					+ "VS_avePerCoreCpuUsage enclosed by '\"',VS_peakPerCoreCpuUsage enclosed by '\"') "
					+ "SOURCE Hdfs(url='http://192.85.247.104:50070/webhdfs/v1/test/loadcsv/input/MyCore_.csv*',username='dbadmin') delimiter ',';";
			String sql2 = "copy sgsiwf.MyCore1_(endTime enclosed by '\"',duration enclosed by '\"',SubNetwork enclosed by '\"',"
					+ "ManagedElement enclosed by '\"',Machine enclosed by '\"',MyCore enclosed by '\"',UUID enclosed by '\"',"
					+ "VS_avePerCoreCpuUsage enclosed by '\"',VS_peakPerCoreCpuUsage enclosed by '\"') "
					+ "SOURCE Hdfs(url='http://192.85.247.104:50070/webhdfs/v1/test/loadcsv/input/MyCore1_.csv*',username='dbadmin') delimiter ',';";
			assertTrue(sqls.contains(sql1));
			assertTrue(sqls.contains(sql2));
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
