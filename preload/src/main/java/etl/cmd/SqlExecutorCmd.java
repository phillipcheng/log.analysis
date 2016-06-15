package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.cmd.dynschema.LogicSchema;
import etl.engine.ETLCmd;
import etl.util.Util;

public class SqlExecutorCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(SqlExecutorCmd.class);
	
	public static final String cfgkey_webhdfs="hdfs.webhdfs.root";
	public static final String cfgkey_systemAttrs_name="systemAttrs.name";
	
	private String webhdfsRoot;
	private String userName;
	private String csvFolder;
	private String prefix;//used as dbschema name
	private String schemaFileName;
	private LogicSchema logicSchema;
	public String[] systemFieldNames;
	
	public SqlExecutorCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		pc.getString(DynSchemaCmd.cfgkey_schema_history_folder);
		this.csvFolder = pc.getString(DynSchemaCmd.cfgkey_csv_folder);
		this.prefix = pc.getString(DynSchemaCmd.cfgkey_prefix);
		this.schemaFileName = pc.getString(DynSchemaCmd.cfgkey_schema_folder) + prefix +"." + DynSchemaCmd.schema_name;
		this.webhdfsRoot = pc.getString(cfgkey_webhdfs);
		this.userName = pc.getString(Util.key_db_user);
		this.logicSchema = (LogicSchema) Util.fromDfsFile(fs, schemaFileName, LogicSchema.class);
		this.systemFieldNames = pc.getStringArray(cfgkey_systemAttrs_name);
	}
	
	private String getOutputDataFileNameWithoutFolder(String tableName, String timeStr){
		return timeStr + "_" + tableName + ".csv";
	}
	
	@Override
	public List<String> process(String param, Mapper<Object, Text, Text, NullWritable>.Context context) {
		//1. execute sql to update db and load data to db
		if (dynCfgMap.containsKey(DynSchemaCmd.dynCfg_Key_CREATETABLE_SQL_FILE)){
			//execute the schemas
			String createtablesql_name = dynCfgMap.get(DynSchemaCmd.dynCfg_Key_CREATETABLE_SQL_FILE).get(0);
			List<String> sqls = Util.stringsFromDfsFile(fs, createtablesql_name);
			Util.executeSqls(sqls, pc);
		}
		//2. load csv files
		List<String> tablesUsed = dynCfgMap.get(DynSchemaCmd.dynCfg_Key_TABLES_USED);
		List<String> csvFiles = new ArrayList<String>();
		List<String> copysqls = new ArrayList<String>();
		for (String tn: tablesUsed){
			List<String> fieldNameList = new ArrayList<String>();
			fieldNameList.addAll(Arrays.asList(systemFieldNames));
			fieldNameList.addAll(logicSchema.getObjParams(tn));
			fieldNameList.addAll(logicSchema.getAttrNames(tn));
			csvFiles.add(getOutputDataFileNameWithoutFolder(tn, wfid));
			String csvFileName = getOutputDataFileNameWithoutFolder(tn, wfid);
			String copySql = Util.genCopyHdfsSql(fieldNameList, tn, prefix, webhdfsRoot, csvFolder + csvFileName, userName);
			copysqls.add(copySql);
		}
		Util.executeSqls(copysqls, pc);
		return null;
	}
}