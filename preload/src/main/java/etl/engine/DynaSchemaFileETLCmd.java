package etl.engine;

import etl.cmd.dynschema.LogicSchema;
import etl.util.Util;

public abstract class DynaSchemaFileETLCmd extends FileETLCmd{

	public static final String cfgkey_schema_file="schema.file";
	public static final String cfgkey_old_table="old.table";
	public static final String cfgkey_new_table="new.table";
	public static final String cfgkey_db_prefix="db.prefix"; //db schema
	public static final String cfgkey_schema_history_folder="schema-history-folder";

	protected String schemaFile;
	protected String oldTable;
	protected String newTable;
	protected String dbPrefix;
	protected String schemaHistoryFolder;
	protected LogicSchema logicSchema;
	
	
	public DynaSchemaFileETLCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.schemaFile = pc.getString(cfgkey_schema_file, null);
		this.oldTable = pc.getString(cfgkey_old_table, null);
		this.newTable = pc.getString(cfgkey_new_table, null);
		this.dbPrefix = pc.getString(cfgkey_db_prefix, null);
		this.schemaHistoryFolder = pc.getString(cfgkey_schema_history_folder, null);
		if (this.schemaFile!=null){
			this.logicSchema = (LogicSchema) Util.fromDfsJsonFile(fs, schemaFile, LogicSchema.class);
		}
	}
}
