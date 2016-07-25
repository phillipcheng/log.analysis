package etl.cmd;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.util.DBUtil;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class LoadDataCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(LoadDataCmd.class);
	
	public static final String cfgkey_webhdfs="hdfs.webhdfs.root";
	public static final String cfgkey_csvfolder = "csv.folder";
	public static final String cfgkey_csvfile = "csv.file";
	public static final String cfgkey_use_wfid = "use.wfid";
	public static final String cfgkey_load_sql = "load.sql";
	
	private String webhdfsRoot;
	private String userName;
	private String csvFolder;
	private String csvFile;
	private boolean useWfid;
	private String loadSql;
	
	public LoadDataCmd(String wfid, String staticCfg, String dynCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, dynCfg, defaultFs, otherArgs);
		this.csvFolder = pc.getString(cfgkey_csvfolder, null);
		this.csvFile = pc.getString(cfgkey_csvfile, null);
		this.webhdfsRoot = pc.getString(cfgkey_webhdfs);
		this.userName = pc.getString(DBUtil.key_db_user);
		this.useWfid = pc.getBoolean(cfgkey_use_wfid, false);
		this.loadSql = pc.getString(cfgkey_load_sql);
	}

	@Override
	public List<String> sgProcess() {
		List<String> logInfo = new ArrayList<String>();
		List<String> copysqls = new ArrayList<String>();
		String sql = null;
		if (csvFolder!=null){
			if (useWfid){
				sql = String.format("%s SOURCE Hdfs(url='%s%s%s/part-*',username='%s') delimiter ','", loadSql, webhdfsRoot, csvFolder, wfid, userName);
			}else{
				sql = String.format("%s SOURCE Hdfs(url='%s%s/part-*',username='%s') delimiter ','", loadSql, webhdfsRoot, csvFolder, userName);
			}
		}else{
			//use csvFile
			String fileName = (String) ScriptEngineUtil.eval(csvFile, VarType.STRING, null);
			sql = String.format("%s SOURCE Hdfs(url='%s%s',username='%s') delimiter ','", loadSql, webhdfsRoot, fileName, userName);
		}
		logger.info("sql:" + sql);
		copysqls.add(sql);
		
		int rowsAdded = DBUtil.executeSqls(copysqls, pc);
		logInfo.add(rowsAdded+"");
		return  logInfo;
	}
}