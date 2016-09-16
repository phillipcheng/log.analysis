package etl.cmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.ScriptEngineUtil;
import etl.util.Util;
import etl.util.VarType;

public abstract class SchemaFileETLCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = Logger.getLogger(SchemaFileETLCmd.class);

	public static final String cfgkey_schema_file="schema.file";
	public static final String cfgkey_db_prefix="db.prefix"; //db schema
	public static final String cfgkey_create_sql="create.sql";
	public static final String cfgkey_db_type="db.type";
	public static final String cfgkey_file_table_map="file.table.map";
	
	//system variable map
	public static final String VAR_LOGIC_SCHEMA="logicSchema"; //
	public static final String VAR_DB_PREFIX="dbPrefix";//
	public static final String VAR_DB_TYPE="dbType";//

	protected String schemaFile;
	protected String schemaFileName;
	protected String dbPrefix;
	protected LogicSchema logicSchema;
	protected String createTablesSqlFileName;
	protected String strFileTableMap;
	protected transient CompiledScript expFileTableMap;
	
	private DBType dbtype = DBType.NONE;
	
	public SchemaFileETLCmd(){
	}
	
	public SchemaFileETLCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfid, staticCfg, defaultFs, otherArgs);
	}
	
	public void init(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super.init(wfid, staticCfg, defaultFs, otherArgs);
		this.schemaFile = pc.getString(cfgkey_schema_file, null);
		this.dbPrefix = pc.getString(cfgkey_db_prefix, null);
		this.getSystemVariables().put(VAR_DB_PREFIX, dbPrefix);
		logger.info(String.format("schemaFile: %s", schemaFile));
		if (this.schemaFile!=null){
			try{
				Path schemaFilePath = new Path(schemaFile);
				if (fs.exists(schemaFilePath)){
					schemaFileName = schemaFilePath.getName();
					this.logicSchema = (LogicSchema) Util.fromDfsJsonFile(fs, schemaFile, LogicSchema.class);
				}else{
					this.logicSchema = new LogicSchema();
					logger.warn(String.format("schema file %s not exists.", schemaFile));
				}
				this.getSystemVariables().put(VAR_LOGIC_SCHEMA, logicSchema);
			}catch(Exception e){
				logger.error("", e);
			}
		}
		String createSqlExp = pc.getString(cfgkey_create_sql, null);
		if (createSqlExp!=null)
			this.createTablesSqlFileName = (String) ScriptEngineUtil.eval(createSqlExp, VarType.STRING, super.getSystemVariables());
		String strDbType = pc.getString(cfgkey_db_type, null);
		this.getSystemVariables().put(VAR_DB_TYPE, strDbType);
		if (strDbType!=null){
			dbtype = DBType.fromValue(strDbType);
		}
		strFileTableMap = pc.getString(cfgkey_file_table_map, null);
		logger.info(String.format("fileTableMap:%s", strFileTableMap));
		if (strFileTableMap!=null){
			expFileTableMap = ScriptEngineUtil.compileScript(strFileTableMap);
			logger.info(String.format("fileTableMapExp:%s", expFileTableMap));
		}
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
			if (dbtype != DBType.NONE){
				DBUtil.executeSqls(createTableSqls, pc);
			}
		}
		//gen report info		
		List<String> loginfo = new ArrayList<String>();
		loginfo.add(createTableSqls.size()+"");
		return loginfo;
	}

	public DBType getDbtype() {
		return dbtype;
	}

	public void setDbtype(DBType dbtype) {
		this.dbtype = dbtype;
	}
	
	public List<String> getCreateSqls(){
		List<String> sqls = new ArrayList<String>();
		for (String tn: logicSchema.getAttrNameMap().keySet()){
			List<String> attrNames = logicSchema.getAttrNames(tn);
			List<FieldType> attrTypes = logicSchema.getAttrTypes(tn);
			String sql = DBUtil.genCreateTableSql(attrNames, attrTypes, tn, dbPrefix, dbtype);
			sqls.add(sql);
		}
		return sqls;
	}
	
	public List<String> getDropSqls(){
		List<String> sqls = new ArrayList<String>();
		for (String tn: logicSchema.getAttrNameMap().keySet()){
			String sql = DBUtil.genDropTableSql(tn, dbPrefix);
			sqls.add(sql);
		}
		return sqls;
	}
	
	public String getTableName(Mapper<LongWritable, Text, Text, Text>.Context context){
		String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		this.getSystemVariables().put(VAR_NAME_FILE_NAME, inputFileName);
		String tableName = inputFileName;
		if (expFileTableMap!=null){
			tableName = ScriptEngineUtil.eval(expFileTableMap, this.getSystemVariables());
		}
		return tableName;
	}

	public LogicSchema getLogicSchema() {
		return logicSchema;
	}

	public void setLogicSchema(LogicSchema logicSchema) {
		this.logicSchema = logicSchema;
	}

	public String getSchemaFileName() {
		return schemaFileName;
	}

	public void setSchemaFileName(String schemaFileName) {
		this.schemaFileName = schemaFileName;
	}
}
