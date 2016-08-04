package etl.engine;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import etl.cmd.dynschema.LogicSchema;
import etl.util.DBUtil;
import etl.util.ScriptEngineUtil;
import etl.util.Util;
import etl.util.VarType;

public abstract class DynaSchemaFileETLCmd extends FileETLCmd{

	public static final String cfgkey_schema_file="schema.file";
	public static final String cfgkey_db_prefix="db.prefix"; //db schema
	public static final String cfgkey_create_sql="create.sql";

	protected String schemaFile;
	protected String dbPrefix;
	protected LogicSchema logicSchema;
	private String createTablesSqlFileName;
	
	private boolean exeSql=true;
	
	
	public DynaSchemaFileETLCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		this.schemaFile = pc.getString(cfgkey_schema_file, null);
		this.dbPrefix = pc.getString(cfgkey_db_prefix, null);
		logger.info(String.format("schemaFile: %s", schemaFile));
		if (this.schemaFile!=null){
			try{
				if (fs.exists(new Path(schemaFile))){
					this.logicSchema = (LogicSchema) Util.fromDfsJsonFile(fs, schemaFile, LogicSchema.class);
				}else{
					this.logicSchema = new LogicSchema();
					logger.warn(String.format("schema file %s not exists.", schemaFile));
				}
			}catch(Exception e){
				logger.error("", e);
			}
		}
		String createSqlExp = pc.getString(cfgkey_create_sql, null);
		if (createSqlExp!=null)
			this.createTablesSqlFileName = (String) ScriptEngineUtil.eval(createSqlExp, VarType.STRING, super.getSystemVariables());
	}
	
	//return loginfo
	public List<String> updateDynSchema(List<String> createTableSqls){
		if (createTableSqls.size()>0){
			//update/create create-table-sql
			logger.info(String.format("create/update table sqls are:%s", createTableSqls));
			Util.appendDfsFile(fs, this.createTablesSqlFileName, createTableSqls);
			//update logic schema file
			Util.toDfsJsonFile(fs, this.schemaFile, logicSchema);
			//execute the sql
			if (exeSql){
				DBUtil.executeSqls(createTableSqls, pc);
			}
		}
		//gen report info		
		List<String> loginfo = new ArrayList<String>();
		loginfo.add(createTableSqls.size()+"");
		return loginfo;
	}

	public boolean isExeSql() {
		return exeSql;
	}

	public void setExeSql(boolean exeSql) {
		this.exeSql = exeSql;
	}
}
