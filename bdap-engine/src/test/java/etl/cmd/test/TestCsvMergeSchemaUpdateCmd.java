package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.List;
import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.CsvMergeCmd;
import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.DBUtil;
import etl.util.SchemaUtils;

public class TestCsvMergeSchemaUpdateCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvMergeSchemaUpdateCmd.class);
	
	@Override
	public String getResourceSubFolder(){
		return "csvmerge/";
	}

	@Test
	public void mergeTables() throws Exception {
		String schemaFolder = "/etltest/merge/";//this is hard coded in the static properties for schema
		String staticCfg = "mergeWithSchema.properties";
		String schemaFile = "multipleTableSchemas.txt";
		//
		String remoteSqlFolder="/etltest/aggrschemaupdate/schemahistory/"; //since this is hard coded in the dynCfg
		String createsqlFile = "createtables.sql_wfid1";//since this is hard coded in the dynCfg
		
		getFs().delete(new Path(schemaFolder), true);
		getFs().mkdirs(new Path(schemaFolder));
		
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
		
		getFs().delete(new Path(remoteSqlFolder), true);
		getFs().mkdirs(new Path(remoteSqlFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
		
		CsvMergeCmd cmd = new CsvMergeCmd("wf1", "wf1", this.getResourceSubFolder() + staticCfg, getDefaultFS(), null);
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		cmd.sgProcess();
		List<String> dropSqls = cmd.getDropSqls();
		DBUtil.executeSqls(dropSqls, cmd.getPc());
		
		//check the schema updated
		LogicSchema ls = SchemaUtils.fromRemoteJsonPath(getDefaultFS(), schemaFolder + schemaFile, LogicSchema.class);
		String newTableName = "MyCoreMerge_";
		assertTrue(ls.hasTable(newTableName));
		List<String> attrs = ls.getAttrNames(newTableName);
		logger.info(String.format("attrs %s for table %s", attrs, newTableName));
		//check the create-sql generated
		String sqlVertica = "create table if not exists sgsiwf.MyCoreMerge_(endTime timestamp,duration varchar(10),"
				+ "SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),MyCore numeric(15,5),UUID varchar(72),"
				+ "MyCore1__VS_avePerCoreCpuUsage numeric(15,5),"
				+ "MyCore1__VS_peakPerCoreCpuUsage numeric(15,5),MyCore__VS_avePerCoreCpuUsage numeric(15,5),"
				+ "MyCore__VS_peakPerCoreCpuUsage numeric(15,5))";
		String sqlHive = "create table if not exists sgsiwf.MyCoreMerge_(endTime timestamp,duration varchar(10),"
				+ "SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),MyCore numeric(15,5),UUID varchar(72),"
				+ "MyCore1__VS_avePerCoreCpuUsage decimal(15,5),"
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
