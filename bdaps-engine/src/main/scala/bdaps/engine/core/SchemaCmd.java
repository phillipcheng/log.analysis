package bdaps.engine.core;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.script.CompiledScript;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.types.StructType;

import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import etl.engine.ETLCmd;
import etl.engine.LogicSchema;
import etl.engine.types.DBType;
import etl.engine.types.LockType;
import etl.engine.types.OutputType;
import etl.util.ConfigKey;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.SchemaUtils;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public abstract class SchemaCmd extends ACmd{
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LogManager.getLogger(SchemaCmd.class);
	private static final String SCHEMA_TMP_FILENAME_EXTENSION = "tmp";
	private static final Random RANDOM_GEN = new Random();

	//cfgkey
	public static final @ConfigKey String cfgkey_schema_file="schema.file";
	public static final @ConfigKey String cfgkey_create_sql="create.sql";
	public static final @ConfigKey String cfgkey_db_prefix="db.prefix"; //db schema
	public static final @ConfigKey(type=DBType.class,defaultValue="none") String cfgkey_db_type="db.type";
	public static final @ConfigKey(type=OutputType.class,defaultValue="multiple") String cfgkey_output_type="output.type";
	public static final @ConfigKey(type=LockType.class,defaultValue="jvm") String cfgkey_lock_type="lock.type";
	public static final @ConfigKey String cfgkey_zookeeper_url="zookeeper.url";
	public static final @ConfigKey String cfgkey_file_table_map="file.table.map";
	
	//system variable map
	public static final String VAR_LOGIC_SCHEMA="logicSchema"; //
	public static final String VAR_DB_PREFIX="dbPrefix";//
	public static final String VAR_DB_TYPE="dbType";//

	protected String schemaFile;
	protected String schemaFileName;
	protected String dbPrefix;
	protected LogicSchema logicSchema;
	protected Map<String, StructType> sparkSqlSchemaMap=null;
	protected String createTablesSqlFileName;
	protected OutputType outputType = OutputType.multiple;
	protected String strFileTableMap;
	protected transient CompiledScript expFileTableMap;
	
	private DBType dbtype = DBType.NONE;
	private LockType lockType = LockType.jvm;
	private String zookeeperUrl=null;
	private transient CuratorFramework client;
	
	public static int ZK_CONNECTION_TIMEOUT = 120000;//2 minutes
	
	public SchemaCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs){
		super(wfName, wfid, staticCfg, prefix, defaultFs);
	}
	
	@Override
	public void init(){
		super.init();
		this.schemaFile = pc().getString(cfgkey_schema_file, null);
		this.dbPrefix = pc().getString(cfgkey_db_prefix, null);
		systemVariables().put(VAR_DB_PREFIX, dbPrefix);

		this.lockType = LockType.valueOf(pc().getString(cfgkey_lock_type, LockType.jvm.toString()));
		this.zookeeperUrl = pc().getString(cfgkey_zookeeper_url, null);
		
		if (this.lockType==LockType.zookeeper && zookeeperUrl != null) {
			client = CuratorFrameworkFactory.newClient(zookeeperUrl, new ExponentialBackoffRetry(1000, 3));
	        client.start();
		} else if (this.lockType==LockType.zookeeper && zookeeperUrl == null) {
			logger.error(String.format("must specify %s for locktype:%s", cfgkey_zookeeper_url, lockType));
		}
		//load schema	
		logger.info(String.format("start load schemaFile: %s", schemaFile));
		if (this.schemaFile!=null){
			Path schemaFilePath = new Path(schemaFile);
			if (SchemaUtils.existsRemoteJsonPath(defaultFs(), schemaFile)){
				schemaFileName = schemaFilePath.getName();
				this.logicSchema = SchemaUtils.fromRemoteJsonPath(defaultFs(), schemaFile, LogicSchema.class);
				if (useSparkSql()){
					loadSparkSchema();
				}
			}else{
				this.logicSchema = SchemaUtils.newRemoteInstance(defaultFs(), schemaFile);
				logger.warn(String.format("schema file %s not exists.", schemaFile));
			}
			systemVariables().put(VAR_LOGIC_SCHEMA, logicSchema);
		}
		logger.info(String.format("end load schemaFile: %s", schemaFile));
	
		String createSqlExp = pc().getString(cfgkey_create_sql, null);
		if (createSqlExp!=null)
			this.createTablesSqlFileName = (String) ScriptEngineUtil.eval(createSqlExp, VarType.STRING, systemVariables());
		String strDbType = pc().getString(cfgkey_db_type, null);
		systemVariables().put(VAR_DB_TYPE, strDbType);
		if (strDbType!=null){
			dbtype = DBType.fromValue(strDbType);
		}
		String strOutputType = pc().getString(cfgkey_output_type, null);
		if (strOutputType!=null){
			outputType = OutputType.valueOf(strOutputType);
		}
		strFileTableMap = pc().getString(cfgkey_file_table_map, null);
		logger.info(String.format("fileTableMap:%s", strFileTableMap));
		if (strFileTableMap!=null){
			expFileTableMap = ScriptEngineUtil.compileScript(strFileTableMap);
			logger.info(String.format("fileTableMapExp:%s", expFileTableMap));
		}
	}
	
	private void loadSparkSchema(){
		sparkSqlSchemaMap = new HashMap<String, StructType>();
		for (String tn: logicSchema.getTableNames()){
			sparkSqlSchemaMap.put(tn, SchemaUtils.convertToSparkSqlSchema(logicSchema, tn));
		}
	}
	
	public StructType getSparkSqlSchema(String tableName){
		return sparkSqlSchemaMap.get(tableName);
	}

	public DBType getDbtype() {
		return dbtype;
	}

	public void setDbtype(DBType dbtype) {
		this.dbtype = dbtype;
	}
	
	public String getTableNameSetPathFileName(String pathName){
		systemVariables().put(ACmdConst.VAR_NAME_PATH_NAME(), pathName);
		int lastSep = pathName.lastIndexOf("/");
		String fileName = pathName.substring(lastSep+1);
		systemVariables().put(ACmdConst.VAR_NAME_FILE_NAME(), fileName);
		if (expFileTableMap!=null){
			return ScriptEngineUtil.eval(expFileTableMap, systemVariables());
		}else{
			return pathName;
		}
	}
	
	@Override
	public String mapKey(String key){
		if (key==null){
			return ETLCmd.SINGLE_TABLE;
		}
		String ret = getTableNameSetPathFileName(key);
		if (ret==null){
			return key;
		}else{
			return ret;
		}
	}
	
	public String getPathName(Mapper<LongWritable, Text, Text, Text>.Context context){
		if (context.getInputSplit() instanceof FileSplit){
			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			systemVariables().put(ACmdConst.VAR_NAME_PATH_NAME(), pathName);
			return pathName;
		}else{
			logger.warn(String.format("can't get path from split:%s", context.getInputSplit().getClass()));
			return null;
		}
	}

	//
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

	public OutputType getOutputType() {
		return outputType;
	}

	public void setOutputType(OutputType outputType) {
		this.outputType = outputType;
	}
}
