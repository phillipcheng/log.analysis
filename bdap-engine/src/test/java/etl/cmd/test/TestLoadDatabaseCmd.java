package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.LoadDataCmd;
import etl.engine.types.DBType;
import etl.engine.types.InputFormatType;
import etl.input.CombineFileNameInputFormat;
import etl.input.FilenameInputFormat;
import etl.util.DBUtil;
import scala.Tuple2;

public class TestLoadDatabaseCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestLoadDatabaseCmd.class);
	public static final String testCmdClass="etl.cmd.LoadDataCmd";
	
	public String getResourceSubFolder(){
		return "loadcsv/";
	}
	
	private void loadDynSchemaFun() throws Exception{

		String staticCfgName = "loadcsvds1.properties";
		String wfid="wfid1";
		String localSchemaFileName = "test1_schemas.txt";
		String csvFileName = "MyCore_.csv";

		String inputFolder = "/test/loadcsv/input/";
		String schemaFolder="/test/loadcsv/schema/";
		
		//generate all the data files
		getFs().delete(new Path(inputFolder), true);
		getFs().delete(new Path(schemaFolder), true);
		//
		getFs().mkdirs(new Path(inputFolder));
		getFs().mkdirs(new Path(schemaFolder));
		//copy schema file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
		//copy csv file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(inputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
		//run cmd
		LoadDataCmd cmd = new LoadDataCmd("wf1", wfid, this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);
		
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		cmd.sgProcess();
		List<String> sqls = cmd.getSgCopySql();
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
		String staticCfgName = "loadcsvdsexp.properties";
		String wfid="wfid1";
		String prefix = "sgsiwf";
		String localSchemaFileName = "multipleTableSchemas.txt";
		String[] csvFileNames = new String[]{"MyCore_.csv", "MyCore1_.csv"};

		String inputFolder = "/test/loadcsv/input/";
		String schemaFolder="/test/loadcsv/schema/";
		
		//generate all the data files
		getFs().delete(new Path(inputFolder), true);
		getFs().delete(new Path(schemaFolder), true);
		//
		getFs().mkdirs(new Path(inputFolder));
		getFs().mkdirs(new Path(schemaFolder));
		//copy schema file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
		//copy csv file
		for (String csvFileName: csvFileNames){
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(inputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
		}
		//run cmd
		LoadDataCmd cmd = new LoadDataCmd("wf1", wfid, this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		cmd.sgProcess();
		List<String> sqls = cmd.getSgCopySql();
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
			logger.info(sqlVertica1);
			assertTrue(sqls.contains(sqlVertica1));
			logger.info(sqlVertica2);
			assertTrue(sqls.contains(sqlVertica2));
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
	
	@Test
	public void testLoadDataMR1Reducer() throws Exception{
		String staticCfgName = "loadcsvmr.properties";
		String remoteCsvInputFolder = "/test/loadcsv/input/";
		String remoteCsvOutputFolder = "/test/loadcsv/output/";
		String schemaFolder="/test/loadcsv/schema/";
		String localSchemaFileName = "multipleTableSchemas.txt";
		String[] csvFileNames = new String[]{"MyCore_.csv", "MyCore1_.csv"};
		
		//generate all the data files
		getFs().delete(new Path(remoteCsvInputFolder), true);
		getFs().delete(new Path(remoteCsvOutputFolder), true);
		getFs().delete(new Path(schemaFolder), true);
		//
		getFs().mkdirs(new Path(remoteCsvInputFolder));
		getFs().mkdirs(new Path(remoteCsvOutputFolder));
		getFs().mkdirs(new Path(schemaFolder));
		//copy schema file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
		//copy csv file
		for (String csvFileName: csvFileNames){
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFileName), new Path(remoteCsvInputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
		}
		
		LoadDataCmd cmd = new LoadDataCmd("wf1", "wfid1", this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);
		
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(remoteCsvInputFolder, csvFileNames));
		
		List<String> output = super.mrTest(rfifs, remoteCsvOutputFolder, staticCfgName, testCmdClass, FilenameInputFormat.class);
		logger.info("Output is:"+output);
		
		List<String> fl = HdfsUtil.listDfsFile(getFs(), "/test/loadcsv/output");
		//assert
		logger.info(fl);
	}
	
	@Test
	public void testLoadDataMR5Reducer() throws Exception{
		String staticCfgName = "loadcsvmr2.properties";
		String remoteCsvInputFolder = "/test/loadcsv/input/";
		String remoteCsvOutputFolder = "/test/loadcsv/output/";
		String schemaFolder="/test/loadcsv/schema/";
		String localSchemaFileName = "multipleTableSchemas2.txt";
		String[] csvFileNames = new String[]{"MyCore_.csv", "MyCore1_.csv"};
		
		//generate all the data files
		getFs().delete(new Path(remoteCsvInputFolder), true);
		getFs().delete(new Path(remoteCsvOutputFolder), true);
		//
		//copy schema file
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
		//copy csv file
		for (String csvFileName: csvFileNames){
			getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName), new Path(remoteCsvInputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
		}
		
		LoadDataCmd cmd = new LoadDataCmd("wf1", "wfid1", this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);
		
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(remoteCsvInputFolder, csvFileNames));
		
		List<String> output = super.mrTest(rfifs, remoteCsvOutputFolder, staticCfgName, testCmdClass, CombineFileNameInputFormat.class, 5);
		logger.info("Output is:"+output);
		assertTrue(output.size()>0);
		
	}
	
	@Test
	public void testSpark1() throws Exception{
		String staticCfgName = "loadcsv.spark.properties";
		String localSchemaFileName = "test1_schemas.txt";
		String[] csvFileNames = new String[]{"MyCore_.csv"};
		
		String inputFolder = "/test/loadcsv/input/";
		String schemaFolder="/test/loadcsv/schema/";
		
		//copy schema file
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
		List<String> keys = super.sparkTestKVKeys(inputFolder, csvFileNames, staticCfgName, etl.cmd.LoadDataCmd.class, 
				InputFormatType.FileName);
		//check hdfs
		logger.info(String.format("output keys:\n %s", String.join("\n", keys)));
		assertTrue(keys.contains("MyCore_"));
		
	}
	
	@Test
	public void testLoadDataWithPipeLineDelimiter() throws Exception{
		String staticCfgName = "loadcsvmrPipelineDelimiter.properties";
		String remoteCsvInputFolder = "/test/loadcsv/input/";
		String remoteCsvOutputFolder = "/test/loadcsv/output/";
		String schemaFolder="/test/loadcsv/schema/";
		String localSchemaFileName = "multipleTableSchemas2.txt";
		String[] csvFileNames = new String[]{"MyCore_Pipeline.csv"};
		
		//generate all the data files
		getFs().delete(new Path(remoteCsvInputFolder), true);
		getFs().delete(new Path(remoteCsvOutputFolder), true);
		//
		//copy schema file
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
		//copy csv file
		for (String csvFileName: csvFileNames){
			getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName), new Path(remoteCsvInputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
		}
		
		LoadDataCmd cmd = new LoadDataCmd("wf1", "wfid1", this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);
		
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(remoteCsvInputFolder, csvFileNames));
		
		List<String> output = super.mrTest(rfifs, remoteCsvOutputFolder, staticCfgName, testCmdClass, CombineFileNameInputFormat.class, 5);
		logger.info("Output is:"+output);
		assertTrue(output.size()>0);
		
	}
	
	@Test
	public void testBeforeLoadTableSQLExecution() throws Exception{
		String staticCfgName = "beforeLoadTableSqlExecution.properties";
		String remoteCsvInputFolder = "/test/loadcsv/input/";
		String remoteCsvOutputFolder = "/test/loadcsv/output/";
		String schemaFolder="/test/loadcsv/schema/";
		String localSchemaFileName = "multipleTableSchemas2.txt";
		String[] csvFileNames = new String[]{"MyCore_Pipeline.csv"};
		
		//generate all the data files
		getFs().delete(new Path(remoteCsvInputFolder), true);
		getFs().delete(new Path(remoteCsvOutputFolder), true);
		//
		//copy schema file
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
		//copy csv file
		for (String csvFileName: csvFileNames){
			getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName), new Path(remoteCsvInputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
		}
		
		LoadDataCmd cmd = new LoadDataCmd("wf1", "wfid1", this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);
		
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		
		List<String> sqls=new ArrayList<String>();
		sqls.add("insert into sgsiwf.MyCore_ (Machine) values('111000')");
		DBUtil.executeSqls(sqls, cmd.getPc());
		
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(remoteCsvInputFolder, csvFileNames));
		
		List<String> output = super.mrTest(rfifs, remoteCsvOutputFolder, staticCfgName, testCmdClass, CombineFileNameInputFormat.class, 5);
		logger.info("Output is:"+output);
		assertTrue(output.size()>0);
		
		List<String> contents = DBUtil.checkCsv("select count(*) from sgsiwf.MyCore_ where Machine='111000';", this.getPc(), 1, 1, ",");
		long size=Long.parseLong(contents.get(0));
		assertTrue(size==0);
		
	}
	
	@Test
	public void cleanTable() throws Exception{
		String staticCfgName = "cleanTable.properties";
		String remoteCsvInputFolder = "/test/loadcsv/input/";
		String remoteCsvOutputFolder = "/test/loadcsv/output/";
		String schemaFolder="/test/loadcsv/schema/";
		String localSchemaFileName = "multipleTableSchemas2.txt";
		String[] csvFileNames = new String[]{"MyCore_Pipeline.csv"};
		
		//generate all the data files
		getFs().delete(new Path(remoteCsvInputFolder), true);
		getFs().delete(new Path(remoteCsvOutputFolder), true);
		//
		//copy schema file
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
		//copy csv file
		for (String csvFileName: csvFileNames){
			getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + csvFileName), new Path(remoteCsvInputFolder + csvFileName));//csv file must be csvfolder/wfid/tableName
		}
		
		LoadDataCmd cmd = new LoadDataCmd("wf1", "wfid1", this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);
		
		DBUtil.executeSqls(cmd.getCreateSqls(), cmd.getPc());
		
		List<String> sqls=new ArrayList<String>();
		sqls.add("insert into sgsiwf.MyCore_ (Machine) values('111000')");
		DBUtil.executeSqls(sqls, cmd.getPc());
		
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(remoteCsvInputFolder, csvFileNames));
		
		List<String> output = super.mrTest(rfifs, remoteCsvOutputFolder, staticCfgName, testCmdClass, CombineFileNameInputFormat.class, 5);
		logger.info("Output is:"+output);
		assertTrue(output.size()>0);
		
		List<String> contents = DBUtil.checkCsv("select count(1) from sgsiwf.MyCore_ where Machine='111000'", this.getPc(), 1, 1, ",");
		long size=Long.parseLong(contents.get(0));
		assertTrue(size==0);
		
	}
}
