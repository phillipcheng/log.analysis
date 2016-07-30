package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.cmd.dynschema.LogicSchema;
import etl.cmd.transform.CsvAggregateCmd;
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
			String dynCfg = "dynschema_test1_dyncfgout_wfid1";
			String schemaFile = "dynschema_test1_schemas.txt";
			//
			String remoteSqlFolder="/test/dynschemacmd/schemahistory/"; //since this is hard coded in the dynCfg
			String createsqlFile = "createtables.sql_wfid1";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() +staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + dynCfg), new Path(remoteCfgFolder + dynCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", remoteCfgFolder + staticCfg, remoteCfgFolder + dynCfg, getDefaultFS(), null);
			cmd.sgProcess();
			
			//check the schema updated
			LogicSchema ls = (LogicSchema) Util.fromDfsJsonFile(getFs(), remoteCfgFolder + schemaFile, LogicSchema.class);
			String newTableName = "MyCore_aggr";
			assertTrue(ls.hasTable(newTableName));
			List<String> attrs = ls.getAttrNames(newTableName);
			assertTrue(attrs.size()==8);
			//check the create-sql
			List<String> sqls = Util.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			String expectedSql="create table if not exists sgsiwf.MyCore_aggr(endTime TIMESTAMP WITH TIMEZONE not null,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),"
					+ "MyCore numeric(15,5),VS_avePerCoreCpuUsage numeric(15,5),VS_peakPerCoreCpuUsage numeric(15,5));";
			logger.info(sqls);
			assertTrue(sqls.contains(expectedSql));
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
	
	private void multipleTablesFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/aggr/cfg/";//this is hard coded in the static properties for schema
			String staticCfg = "csvAggrMultipleFiles.properties";
			String dynCfg = "multipTable_dyncfgout";
			String schemaFile = "multipleTableSchemas.txt";
			//
			String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //since this is hard coded in the dynCfg
			String createsqlFile = "createtables.sql_wfid1";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + dynCfg), new Path(remoteCfgFolder + dynCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", remoteCfgFolder + staticCfg, remoteCfgFolder + dynCfg, getDefaultFS(), null);
			cmd.sgProcess();
			
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
			String expectedSql1="create table if not exists sgsiwf.MyCore_aggr(endTime TIMESTAMP WITH TIMEZONE not null,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),"
					+ "VS_avePerCoreCpuUsage numeric(15,5),VS_peakPerCoreCpuUsage numeric(15,5));";
			String expectedSql2="create table if not exists sgsiwf.MyCore1_aggr(endTime TIMESTAMP WITH TIMEZONE not null,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),"
					+ "MyCore numeric(15,5),"
					+ "VS_avePerCoreCpuUsage numeric(15,5),VS_peakPerCoreCpuUsage numeric(15,5));";
			logger.info(sqls);
			assertTrue(sqls.contains(expectedSql1));
			assertTrue(sqls.contains(expectedSql2));
			//check the dynCfg updated
			Map<String, Object> dynCfgMap = (Map<String, Object>) Util.fromDfsJsonFile(getFs(), remoteCfgFolder + dynCfg, Map.class);
			List<String> tablesUsed = (List<String>) dynCfgMap.get(DynSchemaCmd.dynCfg_Key_TABLES_USED);
			logger.info(tablesUsed);
			assertTrue(tablesUsed.contains(newTableName));
			assertTrue(tablesUsed.contains(newTableName1));
		} catch (Exception e) {
			logger.error("", e);
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
}
