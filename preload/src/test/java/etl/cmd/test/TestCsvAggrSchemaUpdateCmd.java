package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.dynschema.LogicSchema;
import etl.cmd.transform.CsvAggrSchemaUpdateCmd;
import etl.util.Util;

public class TestCsvAggrSchemaUpdateCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestCsvAggrSchemaUpdateCmd.class);
	public static final String testCmdClass = "etl.cmd.transform.CsvAggrSchemaUpdateCmd";

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
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + dynCfg), new Path(remoteCfgFolder + dynCfg));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteCfgFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvAggrSchemaUpdateCmd cmd = new CsvAggrSchemaUpdateCmd("wf1", remoteCfgFolder + staticCfg, remoteCfgFolder + dynCfg, getDefaultFS());
			cmd.sgProcess();
			
			//assertion
			LogicSchema ls = (LogicSchema) Util.fromDfsJsonFile(getFs(), remoteCfgFolder + schemaFile, LogicSchema.class);
			String newTableName = cmd.getPc().getString(CsvAggrSchemaUpdateCmd.cfgkey_new_table);
			assertTrue(ls.hasTable(newTableName));
			List<String> attrs = ls.getAttrNames(newTableName);
			assertTrue(attrs.size()==8);
			List<String> sqls = Util.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			String expectedSql="create table if not exists sgsiwf.MyCore_aggr(endTime TIMESTAMP WITH TIMEZONE not null,"
					+ "duration varchar(10),SubNetwork varchar(70),ManagedElement varchar(70),Machine varchar(54),"
					+ "MyCore numeric(15,5),VS_avePerCoreCpuUsage numeric(15,5),VS_peakPerCoreCpuUsage numeric(15,5));";
			logger.info(sqls);
			assertTrue(sqls.contains(expectedSql));
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
}
