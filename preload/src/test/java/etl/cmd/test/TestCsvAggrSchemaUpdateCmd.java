package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.CsvAggregateCmd;
import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.DBUtil;
import etl.util.Util;

public class TestCsvAggrSchemaUpdateCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestCsvAggrSchemaUpdateCmd.class);
	public static final String testCmdClass = "etl.cmd.transform.CsvAggrSchemaUpdateCmd";
	

	@Override
	public String getResourceSubFolder(){
		return "csvaggr/";
	}

	private void test1Fun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/aggrschemaupdate/cfg/";
			String staticCfg = "csvAggrSchemaUpdate1.properties";
			String schemaFile = "dynschema_test1_schemas.txt";
			//
			String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //this is hard coded in static config
			String createsqlFile = "createtables.sql_wfid1";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() +staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
			cmd.sgProcess();
			List<String> dropSqls = cmd.getDropSqls();
			DBUtil.executeSqls(dropSqls, cmd.getPc());
			
			//check the schema updated
			LogicSchema ls = (LogicSchema) Util.fromDfsJsonFile(getFs(), remoteCfgFolder + schemaFile, LogicSchema.class);
			String newTableName = "MyCore_aggr";
			assertTrue(ls.hasTable(newTableName));
			List<String> attrs = ls.getAttrNames(newTableName);
			assertTrue(attrs.size()==8);
			//check the create-sql
			List<String> sqls = Util.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			String expectedSqlVertica="create table if not exists sgsiwf.MyCore_aggr(endTime timestamp,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),"
					+ "MyCore numeric(15,5),VS_avePerCoreCpuUsage numeric(15,5),VS_peakPerCoreCpuUsage numeric(15,5))";
			String expectedSqlHive="create table if not exists sgsiwf.MyCore_aggr(endTime timestamp,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),"
					+ "MyCore decimal(15,5),VS_avePerCoreCpuUsage decimal(15,5),VS_peakPerCoreCpuUsage decimal(15,5)) "
					+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"";
			logger.info(sqls);
			if (cmd.getDbtype()==DBType.HIVE){
				assertTrue(sqls.contains(expectedSqlHive));
			}else{
				assertTrue(sqls.contains(expectedSqlVertica));
			}
			
			//check dynCfg updated
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
	
	private void groupFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/aggrschemaupdate/cfg/";
			String staticCfg = "csvAggrGroupFun1.properties";
			String schemaFile = "dynschema_test1_schemas.txt";
			//
			String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //this is hard coded in static config
			String createsqlFile = "createtables.sql_wfid1";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() +staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
			cmd.sgProcess();
			List<String> dropSqls = cmd.getDropSqls();
			DBUtil.executeSqls(dropSqls, cmd.getPc());
			
			//check the schema updated
			LogicSchema ls = (LogicSchema) Util.fromDfsJsonFile(getFs(), remoteCfgFolder + schemaFile, LogicSchema.class);
			String newTableName = "MyCore_aggr";
			assertTrue(ls.hasTable(newTableName));
			List<String> attrs = ls.getAttrNames(newTableName);
			assertTrue(attrs.size()==9);
			//check the create-sql
			String expectedSqlVertica = "create table if not exists sgsiwf.MyCore_aggr(endTimeHour int,endTimeDay date,duration varchar(10),"
					+ "SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),"
					+ "MyCore numeric(15,5),VS_avePerCoreCpuUsage numeric(15,5),VS_peakPerCoreCpuUsage numeric(15,5))";
			String expectedSqlHive = "create table if not exists sgsiwf.MyCore_aggr(endTimeHour int,endTimeDay date,duration varchar(10),"
					+ "SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),"
					+ "MyCore decimal(15,5),VS_avePerCoreCpuUsage decimal(15,5),VS_peakPerCoreCpuUsage decimal(15,5)) "
					+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"";
			List<String> sqls = Util.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			logger.info(sqls);
			if (cmd.getDbtype()==DBType.HIVE){
				assertTrue(sqls.contains(expectedSqlHive));
			}else{
				assertTrue(sqls.contains(expectedSqlVertica));
			}
		} catch (Exception e) {
			logger.error("", e);
			assertTrue(false);
		}
	}
	
	@Test
	public void testGroupFun() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			groupFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					groupFun();
					return null;
				}
			});
		}
	}
	
	private void multipleTablesFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/aggr/cfg/";//this is hard coded in the static properties for schema
			String staticCfg = "csvAggrMultipleFiles.properties";
			String schemaFile = "multipleTableSchemas.txt";
			//
			String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //since this is hard coded in the dynCfg
			String createsqlFile = "createtables.sql_wfid1";//since this is hard coded in the dynCfg
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
			cmd.sgProcess();
			List<String> dropSqls = cmd.getDropSqls();
			DBUtil.executeSqls(dropSqls, cmd.getPc());
			
			//check the schema updated
			LogicSchema ls = (LogicSchema) Util.fromDfsJsonFile(getFs(), remoteCfgFolder + schemaFile, LogicSchema.class);
			String newTableName = "MyCore_aggr";
			String newTableName1 = "MyCore1_aggr";
			assertTrue(ls.hasTable(newTableName));
			List<String> attrs = ls.getAttrNames(newTableName);
			logger.info(String.format("attrs %s for table %s", attrs, newTableName));
			assertTrue(attrs.size()==6);
			assertTrue(ls.hasTable(newTableName1));
			attrs = ls.getAttrNames(newTableName1);
			logger.info(String.format("attrs %s for table %s", attrs, newTableName1));
			assertTrue(attrs.size()==7);
			//check the create-sql generated
			List<String> sqls = Util.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			String expectedSql1Vertica="create table if not exists sgsiwf.MyCore_aggr(endTime timestamp,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),"
					+ "VS_avePerCoreCpuUsage numeric(15,5),VS_peakPerCoreCpuUsage numeric(15,5))";
			String expectedSql2Vertica="create table if not exists sgsiwf.MyCore1_aggr(endTime timestamp,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),"
					+ "MyCore numeric(15,5),"
					+ "VS_avePerCoreCpuUsage numeric(15,5),VS_peakPerCoreCpuUsage numeric(15,5))";
			String expectedSql1Hive="create table if not exists sgsiwf.MyCore_aggr(endTime timestamp,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),"
					+ "VS_avePerCoreCpuUsage decimal(15,5),VS_peakPerCoreCpuUsage decimal(15,5)) "
					+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"";
			String expectedSql2Hive="create table if not exists sgsiwf.MyCore1_aggr(endTime timestamp,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),"
					+ "MyCore decimal(15,5),"
					+ "VS_avePerCoreCpuUsage decimal(15,5),VS_peakPerCoreCpuUsage decimal(15,5)) "
					+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"";
			logger.info(sqls);
			if (cmd.getDbtype()==DBType.HIVE){
				assertTrue(sqls.contains(expectedSql1Hive));
				assertTrue(sqls.contains(expectedSql2Hive));
			}else{
				assertTrue(sqls.contains(expectedSql1Vertica));
				assertTrue(sqls.contains(expectedSql2Vertica));
			}
		} catch (Exception e) {
			logger.error("", e);
			assertTrue(false);
		}
	}
	
	@Test
	public void testMultipleTables() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			multipleTablesFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					multipleTablesFun();
					return null;
				}
			});
		}
	}
	
	private void mergeTablesFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/aggr/cfg/";//this is hard coded in the static properties for schema
			String staticCfg = "csvAggrMergeTables.properties";
			String schemaFile = "multipleTableSchemas.txt";
			//
			String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //since this is hard coded in the dynCfg
			String createsqlFile = "createtables.sql_wfid1";//since this is hard coded in the dynCfg
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
			cmd.sgProcess();
			List<String> dropSqls = cmd.getDropSqls();
			DBUtil.executeSqls(dropSqls, cmd.getPc());
			
			//check the schema updated
			LogicSchema ls = (LogicSchema) Util.fromDfsJsonFile(getFs(), remoteCfgFolder + schemaFile, LogicSchema.class);
			String newTableName = "MyCoreMerge_";
			assertTrue(ls.hasTable(newTableName));
			List<String> attrs = ls.getAttrNames(newTableName);
			logger.info(String.format("attrs %s for table %s", attrs, newTableName));
			//check the create-sql generated
			String sqlVertica = "create table if not exists sgsiwf.MyCoreMerge_(endTimeHour int,endTimeDay date,duration varchar(10),"
					+ "SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),"
					+ "MyCore1__MyCore numeric(15,5),MyCore1__VS_avePerCoreCpuUsage numeric(15,5),"
					+ "MyCore1__VS_peakPerCoreCpuUsage numeric(15,5),MyCore__VS_avePerCoreCpuUsage numeric(15,5),"
					+ "MyCore__VS_peakPerCoreCpuUsage numeric(15,5))";
			String sqlHive = "create table if not exists sgsiwf.MyCoreMerge_(endTimeHour int,endTimeDay date,duration varchar(10),"
					+ "SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),"
					+ "MyCore1__MyCore decimal(15,5),MyCore1__VS_avePerCoreCpuUsage decimal(15,5),"
					+ "MyCore1__VS_peakPerCoreCpuUsage decimal(15,5),MyCore__VS_avePerCoreCpuUsage decimal(15,5),"
					+ "MyCore__VS_peakPerCoreCpuUsage decimal(15,5)) "
					+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"";
			List<String> sqls = Util.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			logger.info(sqls);
			if (cmd.getDbtype()==DBType.HIVE){
				assertTrue(sqls.contains(sqlHive));
			}else{
				assertTrue(sqls.contains(sqlVertica));
			}
		} catch (Exception e) {
			logger.error("", e);
			assertTrue(false);
		}
	}
	
	@Test
	public void testMergeTables() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			mergeTablesFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					mergeTablesFun();
					return null;
				}
			});
		}
	}
}
