package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.List;
import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.CsvAggregateCmd;
import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.DBUtil;

public class TestCsvAggrSchemaUpdateCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvAggrSchemaUpdateCmd.class);
	

	@Override
	public String getResourceSubFolder(){
		return "csvaggr/";
	}

	@Test
	public void testSchema1() throws Exception {
		String schemaFolder = "/etltest/aggrschemaupdate/cfg/";
		String staticCfg = "csvAggrSchemaUpdate1.properties";
		String schemaFile = "dynschema_test1_schemas.txt";
		//
		String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //this is hard coded in static config
		String createsqlFile = "createtables.sql_wfid1";
		
		getFs().delete(new Path(schemaFolder), true);
		getFs().mkdirs(new Path(schemaFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
		
		getFs().delete(new Path(remoteSqlFolder), true);
		getFs().mkdirs(new Path(remoteSqlFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
		
		CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", "wf1", this.getResourceSubFolder() + staticCfg, getDefaultFS(), null);
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		cmd.sgProcess();
		List<String> dropSqls = cmd.getDropSqls();
		DBUtil.executeSqls(dropSqls, cmd.getPc());
		
		//check the schema updated
		LogicSchema ls = (LogicSchema) HdfsUtil.fromDfsJsonFile(getFs(), schemaFolder + schemaFile, LogicSchema.class);
		String newTableName = "MyCore_aggr";
		assertTrue(ls.hasTable(newTableName));
		List<String> attrs = ls.getAttrNames(newTableName);
		assertTrue(attrs.size()==8);
		//check the create-sql
		List<String> sqls = HdfsUtil.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
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
	}
	
	@Test
	public void testSchemaGroupFun() throws Exception {
		String schemaFolder = "/etltest/aggrschemaupdate/cfg/";
		String staticCfg = "csvAggrGroupFun1.properties";
		String schemaFile = "dynschema_test1_schemas.txt";
		//
		String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //this is hard coded in static config
		String createsqlFile = "createtables.sql_wfid1";
		
		getFs().delete(new Path(schemaFolder), true);
		getFs().mkdirs(new Path(schemaFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
		
		getFs().delete(new Path(remoteSqlFolder), true);
		getFs().mkdirs(new Path(remoteSqlFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
		
		CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", "wf1", this.getResourceSubFolder() + staticCfg, getDefaultFS(), null);
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		cmd.sgProcess();
		List<String> dropSqls = cmd.getDropSqls();
		DBUtil.executeSqls(dropSqls, cmd.getPc());
		
		//check the schema updated
		LogicSchema ls = (LogicSchema) HdfsUtil.fromDfsJsonFile(getFs(), schemaFolder + schemaFile, LogicSchema.class);
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
		List<String> sqls = HdfsUtil.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
		logger.info(sqls);
		if (cmd.getDbtype()==DBType.HIVE){
			assertTrue(sqls.contains(expectedSqlHive));
		}else{
			assertTrue(sqls.contains(expectedSqlVertica));
		}
	}
	
	@Test
	public void testMultipleTables() throws Exception {
		String schemaFolder = "/etltest/aggr/cfg/";//this is hard coded in the static properties for schema
		String staticCfg = "multipleTablesNoMerge.properties";
		String schemaFile = "multipleTableSchemas.txt";
		//
		String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //since this is hard coded in the dynCfg
		String createsqlFile = "createtables.sql_wfid1";//since this is hard coded in the dynCfg
		
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
		
		CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", "wf1", this.getResourceSubFolder() + staticCfg, getDefaultFS(), null);
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		cmd.sgProcess();
		List<String> dropSqls = cmd.getDropSqls();
		DBUtil.executeSqls(dropSqls, cmd.getPc());
		
		//check the schema updated
		LogicSchema ls = (LogicSchema) HdfsUtil.fromDfsJsonFile(getFs(), schemaFolder + schemaFile, LogicSchema.class);
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
		List<String> sqls = HdfsUtil.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
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
	}
	
	@Test
	public void mergeTables() throws Exception {
		String schemaFolder = "/etltest/aggr/cfg/";//this is hard coded in the static properties for schema
		String staticCfg = "csvAggrMergeTables.properties";
		String schemaFile = "multipleTableSchemas.txt";
		//
		String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //since this is hard coded in the dynCfg
		String createsqlFile = "createtables.sql_wfid1";//since this is hard coded in the dynCfg
		
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
		
		CsvAggregateCmd cmd = new CsvAggregateCmd("wf1", "wf1", this.getResourceSubFolder() + staticCfg, getDefaultFS(), null);
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		cmd.sgProcess();
		List<String> dropSqls = cmd.getDropSqls();
		DBUtil.executeSqls(dropSqls, cmd.getPc());
		
		//check the schema updated
		LogicSchema ls = (LogicSchema) HdfsUtil.fromDfsJsonFile(getFs(), schemaFolder + schemaFile, LogicSchema.class);
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
		List<String> sqls = HdfsUtil.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
		logger.info(sqls);
		if (cmd.getDbtype()==DBType.HIVE){
			assertTrue(sqls.contains(sqlHive));
		}else{
			assertTrue(sqls.contains(sqlVertica));
		}
	}
}
