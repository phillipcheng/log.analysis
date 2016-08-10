package etl.cmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import etl.engine.DynaSchemaFileETLCmd;
import etl.util.DBUtil;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class LoadDataCmd extends DynaSchemaFileETLCmd{
	public static final Logger logger = Logger.getLogger(LoadDataCmd.class);
	
	public static final String cfgkey_webhdfs="hdfs.webhdfs.root";
	public static final String cfgkey_csvfile = "csv.file";
	public static final String cfgkey_load_sql = "load.sql";
	public static final String cfgkey_table_names="table.names";
	public static final String cfgkey_csv_suffix ="csv.suffix";
	
	public static final String VAR_ROOT_WEB_HDFS="rootWebHdfs";
	public static final String VAR_USERNAME="userName";
	public static final String VAR_CSV_FILE="csvFileName";
	public static final String VAR_TABLE_NAME="tableName";
	
	private String webhdfsRoot;
	private String userName;
	private String[] csvFile;
	private String[] loadSql;
	private String[] tableNames;
	private String csvSuffix="";
	
	private CompiledScript csCsvFile;
	private CompiledScript csLoadSql;
	
	private boolean execute=true;
	
	public LoadDataCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		this.csvFile = pc.getStringArray(cfgkey_csvfile);
		this.webhdfsRoot = pc.getString(cfgkey_webhdfs);
		this.userName = pc.getString(DBUtil.key_db_user);
		this.loadSql = pc.getStringArray(cfgkey_load_sql);
		this.tableNames = pc.getStringArray(cfgkey_table_names);
		this.csvSuffix = pc.getString(cfgkey_csv_suffix, "");
		
		this.getSystemVariables().put(VAR_ROOT_WEB_HDFS, this.webhdfsRoot);
		this.getSystemVariables().put(VAR_USERNAME, this.userName);
		if (this.tableNames!=null && this.tableNames.length>0){
			csCsvFile = ScriptEngineUtil.compileScript(csvFile[0]);
			csLoadSql = ScriptEngineUtil.compileScript(loadSql[0]);
		}
	}

	public boolean isExecute() {
		return execute;
	}

	public void setExecute(boolean execute) {
		this.execute = execute;
	}
	
	@Override
	public List<String> sgProcess() {
		List<String> logInfo = new ArrayList<String>();
		List<String> copysqls = new ArrayList<String>();
		if (this.schemaFile==null){
			for (int i=0; i<loadSql.length; i++){//execute loadsql and csvFile pair
				String sql = null;
				//use csvFile
				String fileName = (String) ScriptEngineUtil.eval(csvFile[i], VarType.STRING, super.getSystemVariables());
				sql = String.format("%s SOURCE Hdfs(url='%s%s',username='%s') delimiter ','", loadSql[i], webhdfsRoot, fileName, userName);
				logger.info("sql:" + sql);
				copysqls.add(sql);
			}
		}else{
			try{
				if (tableNames==null || tableNames.length==0){//default sql, match all the files against the tables
					String csvFolder = (String) ScriptEngineUtil.eval(csvFile[0], VarType.STRING, super.getSystemVariables());
					logger.info(String.format("csvFolder:%s", csvFolder));
					FileStatus[] fsta = fs.listStatus(new Path(csvFolder));
					Map<String, String> tableUsed = new HashMap<String, String>();//tableName to suffix mapping
					for (String tableName:logicSchema.getAttrNameMap().keySet()){
						for (FileStatus fst: fsta){
							if (fst.getPath().getName().equals(tableName)){
								tableUsed.put(tableName, tableName);
								continue;
							}else if (fst.getPath().getName().equals(tableName+csvSuffix)){
								tableUsed.put(tableName, tableName+csvSuffix);
								continue;
							}
						}
					}
					for (String table:tableUsed.keySet()){
						String sql = DBUtil.genCopyHdfsSql(null, logicSchema.getAttrNames(table), table, 
								dbPrefix, webhdfsRoot, csvFolder + tableUsed.get(table), userName);
						logger.info(String.format("sql:%s", sql));
						copysqls.add(sql);
					}
				}else{
					for (String tableName:tableNames){
						this.getSystemVariables().put(VAR_TABLE_NAME, tableName);
						String csvFileName = ScriptEngineUtil.eval(this.csCsvFile, this.getSystemVariables());
						this.getSystemVariables().put(VAR_CSV_FILE, csvFileName);
						String sql = ScriptEngineUtil.eval(csLoadSql, this.getSystemVariables());
						logger.info(String.format("sql:%s", sql));
						copysqls.add(sql);
					}
				}
			}catch(Exception e){
				logger.error("", e);
			}
		}
		if (execute){
			int rowsAdded = DBUtil.executeSqls(copysqls, pc);
			logInfo.add(rowsAdded+"");
		}else{
			logInfo.addAll(copysqls);
		}
		return  logInfo;
	}
}