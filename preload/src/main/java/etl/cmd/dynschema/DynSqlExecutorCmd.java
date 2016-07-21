package etl.cmd.dynschema;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.util.DBUtil;
import etl.util.Util;

public class DynSqlExecutorCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(DynSqlExecutorCmd.class);
	
	public static final String cfgkey_webhdfs="hdfs.webhdfs.root";
	public static final String cfgkey_systemAttrs_name="systemAttrs.name";
	
	private String webhdfsRoot;
	private String userName;
	private String csvFolder;
	private String prefix;//used as dbschema name
	private String schemaFileName;
	private LogicSchema logicSchema;
	
	public DynSqlExecutorCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.csvFolder = pc.getString(DynSchemaCmd.cfgkey_csv_folder);
		this.prefix = pc.getString(DynSchemaCmd.cfgkey_prefix);
		this.schemaFileName = pc.getString(DynSchemaCmd.cfgkey_schema_folder) + prefix +"." + DynSchemaCmd.schema_name;
		this.webhdfsRoot = pc.getString(cfgkey_webhdfs);
		this.userName = pc.getString(DBUtil.key_db_user);
		this.logicSchema = (LogicSchema) Util.fromDfsJsonFile(fs, schemaFileName, LogicSchema.class);
	}
	
	private String getOutputDataFileNameWithoutFolder(String tableName, String wfid){
		return wfid + "_" + tableName + ".csv";
	}
	
	@Override
	public List<String> sgProcess() {
		//1. execute sql to update db and load data to db
		if (dynCfgMap.containsKey(DynSchemaCmd.dynCfg_Key_CREATETABLE_SQL_FILE)){
			//execute the schemas
			String createtablesql_name = dynCfgMap.get(DynSchemaCmd.dynCfg_Key_CREATETABLE_SQL_FILE).get(0);
			List<String> sqls = Util.stringsFromDfsFile(fs, createtablesql_name);
			DBUtil.executeSqls(sqls, pc);
		}
		//2. load csv files
		List<String> tablesUsed = dynCfgMap.get(DynSchemaCmd.dynCfg_Key_TABLES_USED);
		List<String> csvFiles = new ArrayList<String>();
		List<String> copysqls = new ArrayList<String>();
		for (String tn: tablesUsed){
			List<String> fieldNameList = new ArrayList<String>();
			fieldNameList.addAll(logicSchema.getAttrNames(tn));
			csvFiles.add(getOutputDataFileNameWithoutFolder(tn, wfid));
			String csvFileName = getOutputDataFileNameWithoutFolder(tn, wfid);
			String copySql = DBUtil.genCopyHdfsSql(fieldNameList, tn, prefix, webhdfsRoot, csvFolder + csvFileName, userName);
			copysqls.add(copySql);
		}
		int rowsUpdated = DBUtil.executeSqls(copysqls, pc);
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(rowsUpdated + "");
		return logInfo;
	}
}