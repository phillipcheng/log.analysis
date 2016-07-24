package etl.engine;

import java.util.ArrayList;
import java.util.List;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.cmd.dynschema.LogicSchema;
import etl.util.Util;

public abstract class DynaSchemaFileETLCmd extends FileETLCmd{

	public static final String cfgkey_schema_file="schema.file";
	public static final String cfgkey_old_table="old.table";
	public static final String cfgkey_new_table="new.table";
	public static final String cfgkey_db_prefix="db.prefix"; //db schema

	protected String schemaFile;
	protected String oldTable;
	protected String newTable;
	protected String dbPrefix;
	protected LogicSchema logicSchema;
	
	
	public DynaSchemaFileETLCmd(String wfid, String staticCfg, String dynCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, dynCfg, defaultFs, otherArgs);
		this.schemaFile = pc.getString(cfgkey_schema_file, null);
		this.oldTable = pc.getString(cfgkey_old_table, null);
		this.newTable = pc.getString(cfgkey_new_table, null);
		this.dbPrefix = pc.getString(cfgkey_db_prefix, null);
		logger.info(String.format("schemaFile: %s", schemaFile));
		if (this.schemaFile!=null){
			this.logicSchema = (LogicSchema) Util.fromDfsJsonFile(fs, schemaFile, LogicSchema.class);
		}
	}
	
	//return loginfo
	public List<String> updateDynSchema(List<String> createTableSqls, boolean schemaUpdated){
		logger.info(String.format("schema updated %b", schemaUpdated));
		if (createTableSqls.size()>0){
			logger.info(String.format("create/update table sqls are:%s", createTableSqls));
			//update/create create-table-sql
			String createsqlFileName = (String) dynCfgMap.get(DynSchemaCmd.dynCfg_Key_CREATETABLE_SQL_FILE);
			Util.appendDfsFile(fs, createsqlFileName, createTableSqls);
		}
		//update/create dyncfg
		List<String> tableUsed = (List<String>) dynCfgMap.get(DynSchemaCmd.dynCfg_Key_TABLES_USED);
		if (newTable!=null && !tableUsed.contains(this.newTable)){
			tableUsed.add(this.newTable);
		}
		this.saveDynCfg();
		if (schemaUpdated){
			Util.toDfsJsonFile(fs, this.schemaFile, logicSchema);
		}
		List<String> loginfo = new ArrayList<String>();
		loginfo.add(createTableSqls.size()+"");
		return loginfo;
	}
}
