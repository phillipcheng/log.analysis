package etl.cmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	public static final String cfgkey_csv_suffix ="csv.suffix";
	
	private String webhdfsRoot;
	private String userName;
	private String[] csvFile;
	private String[] loadSql;
	private String csvSuffix="";
	
	public LoadDataCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		this.csvFile = pc.getStringArray(cfgkey_csvfile);
		this.webhdfsRoot = pc.getString(cfgkey_webhdfs);
		this.userName = pc.getString(DBUtil.key_db_user);
		this.loadSql = pc.getStringArray(cfgkey_load_sql);
		this.csvSuffix = pc.getString(cfgkey_csv_suffix, "");
	}

	@Override
	public List<String> sgProcess() {
		List<String> logInfo = new ArrayList<String>();
		List<String> copysqls = new ArrayList<String>();
		if (this.logicSchema==null){
			for (int i=0; i<loadSql.length; i++){
				String sql = null;
				//use csvFile
				String fileName = (String) ScriptEngineUtil.eval(csvFile[i], VarType.STRING, super.getSystemVariables());
				sql = String.format("%s SOURCE Hdfs(url='%s%s',username='%s') delimiter ','", loadSql[i], webhdfsRoot, fileName, userName);
				logger.info("sql:" + sql);
				copysqls.add(sql);
			}
		}else{
			try{
				String csvFolder = (String) ScriptEngineUtil.eval(csvFile[0], VarType.STRING, super.getSystemVariables());
				logger.info(String.format("csvFolder:%s", csvFolder));
				FileStatus[] fsta = fs.listStatus(new Path(csvFolder));
				Map<String, String> tableUsed = new HashMap<String, String>();//tableName to suffix mapping
				for (String tableName:logicSchema.getAttrNameMap().keySet()){
					for (FileStatus fst: fsta){
						if (fst.getPath().getName().equals(tableName)){
							tableUsed.put(tableName, tableName);
							continue;
						}else if (fst.getPath().getName().startsWith(tableName+csvSuffix)){
							tableUsed.put(tableName, tableName+csvSuffix);
							continue;
						}
					}
				}
				for (String table:tableUsed.keySet()){
					String sql = DBUtil.genCopyHdfsSql(logicSchema.getAttrNames(table), table, 
							dbPrefix, webhdfsRoot, csvFolder + tableUsed.get(table), userName);
					logger.info("sql:" + sql);
					copysqls.add(sql);
				}
			}catch(Exception e){
				logger.error("", e);
			}
		}
		int rowsAdded = DBUtil.executeSqls(copysqls, pc);
		logInfo.add(rowsAdded+"");
		return  logInfo;
	}
}