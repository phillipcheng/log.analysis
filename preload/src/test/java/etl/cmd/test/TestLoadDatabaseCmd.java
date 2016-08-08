package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import etl.cmd.LoadDataCmd;
import etl.util.DBUtil;
import etl.util.Util;

public class TestLoadDatabaseCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestLoadDatabaseCmd.class);
	
	public String getResourceSubFolder(){
		return "loadcsv/";
	}
	
	@Before
    public void setUp() {
		setCfgProperties("testETLCmd_192.85.247.104.properties");
		super.setUp();
	}
	
	private void assertTableContent(String dfsConfigFolder, String staticCfgName, String csvFolder, 
			String[] tableNames, String[] csvFileNames, int rowsUpdated){
		PropertiesConfiguration pc = Util.getMergedPCFromDfs(getFs(), dfsConfigFolder + staticCfgName);
		int linesInFile = 0;
		for (int i=0; i<tableNames.length; i++){
			String tableName = tableNames[i];
			String csvFileName = csvFileNames[i];
			BufferedReader br = null;
			try {
				String line = null;
				// fetch db data
				List<String> dbData = DBUtil.checkCsv(String.format("select * from %s",tableName), pc, 0, 0, ",");
				logger.info(dbData);
				// check number of lines in file same as rowsupdated, db
				// data against each line
				br = new BufferedReader(new InputStreamReader(getFs().open(new Path(csvFolder + csvFileName))));
				while ((line = br.readLine()) != null) {
					linesInFile++;
					logger.info(line);
					assertTrue(dbData.contains(line.trim()));
				}
			} catch (Exception e) {
				logger.error("Exception occured due to invalid data-history path", e);
			} finally {
				if (br != null){
					try{
						br.close();
					}catch(Exception e){
						logger.error("", e);
					}
				}
			}
		}
		assertTrue( rowsUpdated == linesInFile);
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
			List<String> numberOfRowsupdated = cmd.sgProcess();
			
			//assertion
			int rowsUpdated = Integer.parseInt(numberOfRowsupdated.get(0));
			assertTableContent(dfsFolder, staticCfgName, csvFolder, new String[]{"lsl_sample"}, new String[]{csvFileName}, rowsUpdated);
		
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
		List<String> numberOfRowsupdated =  cmd.sgProcess();
		
		//assertion
		int rowsUpdated = Integer.parseInt(numberOfRowsupdated.get(0));
		assertTableContent(dfsFolder, staticCfgName, csvFolder, new String[]{"lsl_sample"}, new String[]{csvFileName}, rowsUpdated);
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
		List<String> numberOfRowsupdated =  cmd.sgProcess();
		
		//assertion
		int rowsUpdated = Integer.parseInt(numberOfRowsupdated.get(0));
		assertTableContent(dfsFolder, staticCfgName, csvFolder, new String[]{"lsl_sample", "lsl_sample1"}, 
				csvFileName, rowsUpdated);
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
			List<String> info = cmd.sgProcess();
			logger.info(info);
			int numRows = Integer.parseInt(info.get(0));
			assertTrue(numRows==8);
			
			//checking create table already created
			String sql ="SELECT table_name from tables where table_schema='"+prefix+"' and table_name='MyCore_';";
			PropertiesConfiguration pc = Util.getMergedPCFromDfs(getFs(), dfsCfgFolder + staticCfgName);
			boolean result=DBUtil.checkTableExists(sql,pc);
			assertTrue(result);

			//get csvData to check
			ArrayList<String> csvData=new ArrayList<String>();
			String line,newline=null;
			int startIndex=1,endIndex=5;
			BufferedReader br=null;
			br = new BufferedReader(new FileReader(getLocalFolder() + csvFileName));
			while ((line = br.readLine()) != null) {
				String[] colArray = line.split("\",\"");
				newline="";
				for(int j=startIndex;j<endIndex;j++) {
					if(j==endIndex-1){
						newline=newline+colArray[j];
					}else{
						newline=newline+colArray[j]+" ";
					}
				}
				csvData.add(newline);  
			}
			br.close();
			// get table data
			ArrayList<String> cols=new ArrayList<String>() ;
			sql = "SELECT * from sgsiwf.MyCore_;";
			cols=DBUtil.checkCsv(sql, pc, startIndex+1, endIndex, " ");
			logger.info("The Comparation status :"+cols.containsAll(csvData));
			
			//check dbdata has csv data 
			assertTrue(cols.containsAll(csvData));
			logger.info("The results are verified successfully");
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
}
