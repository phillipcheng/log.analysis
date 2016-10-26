package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.CsvTransformCmd;
import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.DBUtil;

public class TestCsvTransSchemaUpdateCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvTransSchemaUpdateCmd.class);

	public String getResourceSubFolder(){
		return "csvtrans/";
	}
	
	private void test1Fun() throws Exception {
		try {
			//
			String schemaFolder = "/etltest/transschemaupdate/cfg/";
			String staticCfg = "csvTransSchemaUpdate1.properties";
			String schemaFile = "dynschema_test1_schemas.txt";
			//
			String remoteSqlFolder="/test/dynschemacmd/schemahistory/"; //since this is hard coded in the dynCfg
			String createsqlFile = "createtables.sql_wfid1";
			
			getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvTransformCmd cmd = new CsvTransformCmd("wf1", "wf1", this.getResourceSubFolder() + staticCfg, getDefaultFS(), null);
			List<String> createSqls = cmd.getCreateSqls();
			DBUtil.executeSqls(createSqls, cmd.getPc());
			cmd.sgProcess();
			List<String> dropSqls = cmd.getDropSqls();
			DBUtil.executeSqls(dropSqls, cmd.getPc());
			//assertion
			LogicSchema ls = cmd.getLogicSchema();
			String tableName = "MyCore_";
			assertTrue(ls.hasTable(tableName));
			List<String> attrs = ls.getAttrNames(tableName);
			assertTrue(attrs.contains("aveActiveSubsNum"));
			List<String> sqls = HdfsUtil.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			logger.info("sqls:" + String.join("\n", sqls));
			String expectedSqlVertica = String.format(
					"alter table sgsiwf.%s add column aveActiveSubsNum numeric(15,5)", tableName);
			String expectedSqlHive = String.format(
					"alter table sgsiwf.%s add columns (aveActiveSubsNum decimal(15,5))", tableName);
			if (cmd.getDbtype()==DBType.HIVE){
				assertTrue(sqls.contains(expectedSqlHive));
			}else{
				assertTrue(sqls.contains(expectedSqlVertica));
			}
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
			String schemaFolder = "/etltest/transschemaupdate/cfg/";
			String staticCfg = "csvTransSchemaUpdate2.properties";
			String schemaFile = "dynschema_test2_schemas.txt";
			//
			String remoteSqlFolder="/test/dynschemacmd/schemahistory/"; //since this is hard coded in the dynCfg
			String createsqlFile = "createtables.sql_wfid1";
			
			getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + schemaFile), new Path(schemaFolder + schemaFile));
			
			getFs().delete(new Path(remoteSqlFolder), true);
			getFs().mkdirs(new Path(remoteSqlFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + createsqlFile), new Path(remoteSqlFolder + createsqlFile));
			
			CsvTransformCmd cmd = new CsvTransformCmd("wf1", "wf1", this.getResourceSubFolder() + staticCfg, getDefaultFS(), null);
			DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
			cmd.sgProcess();
			List<String> sqls = HdfsUtil.stringsFromDfsFile(getFs(), remoteSqlFolder + createsqlFile);
			logger.info(sqls);
			List<String> dropSqls = cmd.getDropSqls();
			DBUtil.executeSqls(dropSqls, cmd.getPc());
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
