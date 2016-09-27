package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.script.CompiledScript;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.util.DBType;
import etl.util.DBUtil;
import etl.util.ScriptEngineUtil;

public class LoadDataCmd extends SchemaFileETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(LoadDataCmd.class);
	
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
	private String csvFile;
	private String loadSql;
	private String[] tableNames;
	
	private transient CompiledScript csCsvFile;
	private transient CompiledScript csLoadSql;
	private transient List<String> copysqls;
	
	public LoadDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public LoadDataCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		
		this.csvFile = super.getCfgString(cfgkey_csvfile, null);
		this.webhdfsRoot = super.getCfgString(cfgkey_webhdfs, null);
		this.userName = super.getCfgString(DBUtil.key_db_user, null);
		this.loadSql = super.getCfgString(cfgkey_load_sql, null);
		logger.info(String.format("load sql:%s", loadSql));
		this.tableNames = super.getCfgStringArray(cfgkey_table_names);
		//
		this.getSystemVariables().put(VAR_ROOT_WEB_HDFS, this.webhdfsRoot);
		this.getSystemVariables().put(VAR_USERNAME, this.userName);
		if (this.csvFile!=null){
			csCsvFile = ScriptEngineUtil.compileScript(csvFile);
		}else{
			logger.warn(String.format("csvFile is not specified."));
		}
		if (this.loadSql!=null){
			csLoadSql = ScriptEngineUtil.compileScript(loadSql);
		}else{
			logger.warn(String.format("loadSql is not specified."));
		}
		copysqls = new ArrayList<String>();
	}
	
	@Override
	public List<String> sgProcess() {
		if (this.getFs()==null) init();
		List<String> logInfo = new ArrayList<String>();
		try{
			String sql = null;
			if (logicSchema!=null && (csLoadSql==null||loadSql.contains(VAR_TABLE_NAME))){	
				List<String> tryTables = new ArrayList<String>();
				if (tableNames==null || tableNames.length==0){//default sql, match all the files against the tables
					tryTables.addAll(logicSchema.getAttrNameMap().keySet());
				}else{
					tryTables.addAll(Arrays.asList(tableNames));
				}
			
				for (String tableName:tryTables){
					String csvFileName = null;
					this.getSystemVariables().put(VAR_TABLE_NAME, tableName);
					csvFileName = ScriptEngineUtil.eval(this.csCsvFile, this.getSystemVariables());
					//
					this.getSystemVariables().put(VAR_CSV_FILE, csvFileName);
					
					if (csLoadSql!=null){
						sql = ScriptEngineUtil.eval(csLoadSql, this.getSystemVariables());
					}else{
						sql = DBUtil.genCopyHdfsSql(null, logicSchema.getAttrNames(tableName), tableName, 
								dbPrefix, this.webhdfsRoot, csvFileName, this.userName, this.getDbtype());
					}
					copysqls.add(sql);
				}
			}else{//just evaluate the loadSql
				String csvFileName = ScriptEngineUtil.eval(this.csCsvFile, this.getSystemVariables());
				this.getSystemVariables().put(VAR_CSV_FILE, csvFileName);
				sql = ScriptEngineUtil.eval(csLoadSql, this.getSystemVariables());
				copysqls.add(sql);
			}
			logger.info(String.format("sql:%s", sql));
		}catch(Exception e){
			logger.error("", e);
		}
		
		if (super.getDbtype()!=DBType.NONE){
			int rowsAdded = DBUtil.executeSqls(copysqls, super.getPc());
			logInfo.add(rowsAdded+"");
		}
		
		return  logInfo;
	}

	public List<String> getCopysqls() {
		return copysqls;
	}

	public void setCopysqls(List<String> copysqls) {
		this.copysqls = copysqls;
	}

}