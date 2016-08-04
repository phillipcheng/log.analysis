package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.dynschema.LogicSchema;
import etl.cmd.transform.CsvTransformCmd;
import etl.util.Util;

public class TestCsvTransSchemaUpdateCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestCsvTransSchemaUpdateCmd.class);
	public static final String testCmdClass = "etl.cmd.transform.CsvTransSchemaUpdateCmd";

	public String getResourceSubFolder(){
		return "csvtrans/";
	}
	
	private void test1Fun() throws Exception {
		try {
			//
			String remoteCfgFolder = "/etltest/transschemaupdate/cfg/";
			String staticCfg = "csvTransSchemaUpdate1.properties";
			String schemaFile = "dynschema_test1_schemas.txt";
			//
			String remoteSqlFolder="/test/dynschemacmd/schemahistory/"; //since this is hard coded in the dynCfg
			String createsqlFile = "createtables.sql_wfid1";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvTransformCmd cmd = new CsvTransformCmd("wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			cmd.setExeSql(false);
			cmd.sgProcess();
			
			//assertion
			LogicSchema ls = (LogicSchema) Util.fromDfsJsonFile(getFs(), remoteCfgFolder + schemaFile, LogicSchema.class);
			String tableName = "MyCore_";
			assertTrue(ls.hasTable(tableName));
			List<String> attrs = ls.getAttrNames(tableName);
			assertTrue(attrs.contains("aveActiveSubsNum"));
			List<String> sqls = Util.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			String expectedUpdateSql = String.format("alter table sgsiwf.%s add column aveActiveSubsNum numeric(15,5);", tableName);
			logger.info(sqls);
			assertTrue(sqls.contains(expectedUpdateSql));
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
	
	private void noUpdateFun() throws Exception {
		try {
			//
			String remoteCfgFolder = "/etltest/transschemaupdate/cfg/";
			String staticCfg = "csvTransSchemaUpdate2.properties";
			String schemaFile = "dynschema_test2_schemas.txt";
			//
			String remoteSqlFolder="/test/dynschemacmd/schemahistory/"; //since this is hard coded in the dynCfg
			String createsqlFile = "createtables.sql_wfid1";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvTransformCmd cmd = new CsvTransformCmd("wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			cmd.setExeSql(false);
			cmd.sgProcess();
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testNoUpdate() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			noUpdateFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					noUpdateFun();
					return null;
				}
			});
		}
	}
}
