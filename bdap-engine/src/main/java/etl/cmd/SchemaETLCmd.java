package etl.cmd;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.script.CompiledScript;

import org.apache.commons.lang.ArrayUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
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

import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import etl.engine.ETLCmd;
import etl.engine.LockType;
import etl.engine.LogicSchema;
import etl.engine.OutputType;
import etl.engine.ProcessMode;
import etl.util.DBType;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.SchemaUtils;
import etl.util.ScriptEngineUtil;
import etl.util.VarDef;
import etl.util.VarType;

public abstract class SchemaETLCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;
	private static final String SCHEMA_TMP_FILENAME_EXTENSION = "tmp";
	private static final Random RANDOM_GEN = new Random();
	public static final Logger logger = LogManager.getLogger(SchemaETLCmd.class);

	//cfgkey
	public static final String cfgkey_schema_file="schema.file";
	public static final String cfgkey_file_table_map="file.table.map";
	public static final String cfgkey_create_sql="create.sql";
	public static final String cfgkey_db_prefix="db.prefix"; //db schema
	public static final String cfgkey_db_type="db.type";
	public static final String cfgkey_output_type="output.type";
	public static final String cfgkey_lock_type="lock.type";
	public static final String cfgkey_zookeeper_url="zookeeper.url";
	
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
	protected OutputType outputType = OutputType.multiple;
	
	private DBType dbtype = DBType.NONE;
	private LockType lockType = LockType.zookeeper;
	private String zookeeperUrl=null;
	private CuratorFramework client;
	
	public static int ZK_CONNECTION_TIMEOUT = 120000;//2 minutes
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm, true);
	}
	/**
	 * Additional parameters:
	 * @param loadSchema: if true, load the schema in init
	 * @param zkUrl: if not null, lock before reade
	 */
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, 
			ProcessMode pm, boolean loadSchema){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		this.schemaFile = super.getCfgString(cfgkey_schema_file, null);
		this.dbPrefix = super.getCfgString(cfgkey_db_prefix, null);
		this.getSystemVariables().put(VAR_DB_PREFIX, dbPrefix);

		this.lockType = LockType.valueOf(super.getCfgString(cfgkey_lock_type, LockType.zookeeper.toString()));
		this.zookeeperUrl = super.getCfgString(cfgkey_zookeeper_url, null);
		
		if (this.lockType==LockType.zookeeper && zookeeperUrl != null) {
			client = CuratorFrameworkFactory.newClient(zookeeperUrl, new ExponentialBackoffRetry(1000, 3));
	        client.start();
		} else if (this.lockType==LockType.zookeeper && zookeeperUrl == null) {
			logger.error(String.format("must specify %s for locktype:%s", cfgkey_zookeeper_url, lockType));
		}
		
		if (loadSchema){
			logger.info(String.format("start load schemaFile: %s", schemaFile));
			if (this.schemaFile!=null){
				Path schemaFilePath = new Path(schemaFile);
				if (SchemaUtils.existsRemoteJsonPath(defaultFs, schemaFile)){
					schemaFileName = schemaFilePath.getName();
					this.logicSchema = SchemaUtils.fromRemoteJsonPath(defaultFs, schemaFile, LogicSchema.class);
				}else{
					this.logicSchema = SchemaUtils.newRemoteInstance(defaultFs, schemaFile);
					logger.warn(String.format("schema file %s not exists.", schemaFile));
				}
				this.getSystemVariables().put(VAR_LOGIC_SCHEMA, logicSchema);
			}
			logger.info(String.format("end load schemaFile: %s", schemaFile));
		}
		String createSqlExp = super.getCfgString(cfgkey_create_sql, null);
		if (createSqlExp!=null)
			this.createTablesSqlFileName = (String) ScriptEngineUtil.eval(createSqlExp, VarType.STRING, super.getSystemVariables());
		String strDbType = super.getCfgString(cfgkey_db_type, null);
		this.getSystemVariables().put(VAR_DB_TYPE, strDbType);
		if (strDbType!=null){
			dbtype = DBType.fromValue(strDbType);
		}
		strFileTableMap = super.getCfgString(cfgkey_file_table_map, null);
		logger.info(String.format("fileTableMap:%s", strFileTableMap));
		if (strFileTableMap!=null){
			expFileTableMap = ScriptEngineUtil.compileScript(strFileTableMap);
			logger.info(String.format("fileTableMapExp:%s", expFileTableMap));
		}
		String strOutputType = super.getCfgString(cfgkey_output_type, null);
		if (strOutputType!=null){
			outputType = OutputType.valueOf(strOutputType);
		}
	}
	
	@Override
	public void close() {
		super.close();
		
		if (client != null)
			CloseableUtils.closeQuietly(client);
	}
	
	@Override
	public VarDef[] getCfgVar(){
		return (VarDef[]) ArrayUtils.addAll(super.getCfgVar(), new VarDef[]{});
	}
	
	@Override
	public VarDef[] getSysVar(){
		return (VarDef[]) ArrayUtils.addAll(super.getSysVar(), new VarDef[]{});
	}
	
	public void genSchemaSql(Map<String, List<String>> attrsMap, Map<String, List<FieldType>> attrTypesMap, String schemaFile, String sqlFile){
		List<String> createTableSqls = new ArrayList<String>();
		LogicSchema newls = SchemaUtils.newLocalInstance(schemaFile);
		newls.setAttrNameMap(attrsMap);
		newls.setAttrTypeMap(attrTypesMap);
		for (String newTable: attrsMap.keySet()){
			List<String> newAttrs = attrsMap.get(newTable);
			List<FieldType> newTypes = attrTypesMap.get(newTable);
			createTableSqls.add(DBUtil.genCreateTableSql(newAttrs, newTypes, newTable, dbPrefix, getDbtype())+";\n");
		}
		JsonUtil.toLocalJsonFile(schemaFile, newls);
		try {
			java.nio.file.Path out = Paths.get(sqlFile);
			Files.write(out, createTableSqls, Charset.defaultCharset());
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	private void safeWriteSchemaFile(String defaultFs, String path, boolean directory, LogicSchema schema) {
		String originalPath = path;
		
		if (!directory) {
			path = path + "." + SCHEMA_TMP_FILENAME_EXTENSION + "." + RANDOM_GEN.nextLong();
		}
		
		SchemaUtils.toRemoteJsonPath(defaultFs, path, directory, schema, null);
		
		/* Rename is atomic operation */
		if (!directory) {
			FileContext fsctx = HdfsUtil.getHadoopFsContext(defaultFs);
			try {
				fsctx.rename(new Path(path), new Path(originalPath), Options.Rename.OVERWRITE);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	private LogicSchema createTableSchema(LogicSchema tableSchema, String schemaFile, String name, List<String> attrIds,
			List<String> attrNames, List<FieldType> attrTypes, List<String> loginfo) {
		int i;
		tableSchema.updateTableAttrs(name, attrNames);
		tableSchema.updateTableAttrTypes(name, attrTypes);
		
		if (attrIds != null && attrIds.size() == attrNames.size())
			for (i = 0; i < attrNames.size(); i ++)
				tableSchema.getAttrIdNameMap().put(attrIds.get(i), attrNames.get(i));
		
		//generate create table
		String createTableSql = DBUtil.genCreateTableSql(attrNames, attrTypes, name, 
				dbPrefix, getDbtype());
		
		//update/create create-table-sql
		logger.info(String.format("create/update table sqls are:%s", createTableSql));
		
		List<String> createTableSqls = Arrays.asList(new String[] {createTableSql});
		HdfsUtil.appendDfsFile(fs, this.createTablesSqlFileName, createTableSqls);
		
		//update logic schema file
		if (schemaFile != null)
			safeWriteSchemaFile(defaultFs, schemaFile, tableSchema.isIndex(), tableSchema);
		
		//execute the sql
		if (dbtype != DBType.NONE){
			int result = DBUtil.executeSqls(createTableSqls, super.getPc());
			//gen report info
			loginfo.add(result + ":" + createTableSql);
		}
		
		return tableSchema;
	}
	
	private LogicSchema updateTableSchema(LogicSchema tableSchema, String schemaFile, String name, List<String> attrIds, List<String> attrNames,
			List<FieldType> attrTypes, List<String> loginfo) {
		List<String> existAttrs = tableSchema.getAttrNames(name);
		if (existAttrs.containsAll(attrNames)) {//
			logger.debug(String.format("update nothing for %s", name));
			return null;
			
		} else {
			int i;
			String attrId;
			//update schema, happens only when the schema is updated by external force
			
			//check new attribute
			List<String> newAttrNames = new ArrayList<String>();
			List<String> newAttrIds = new ArrayList<String>();
			List<FieldType> newAttrTypes = new ArrayList<FieldType>();
			for (i = 0; i < attrNames.size(); i++) {//for every attr
				String attrName = attrNames.get(i);
				if (!existAttrs.contains(attrName)) {
					newAttrNames.add(attrName);
					if (attrIds != null && i < attrIds.size()) {
						attrId = attrIds.get(i);
						newAttrIds.add(attrId);
					}
					FieldType ft = null;
					if (attrTypes != null && i < attrTypes.size()) {
						ft = attrTypes.get(i);
					} else {
						ft = DBUtil.guessDBType("");
					}
					newAttrTypes.add(ft);
				}
			}
			
			tableSchema.addAttributes(name, newAttrNames);
			tableSchema.addAttrTypes(name, newAttrTypes);
			
			if (newAttrIds.size() == newAttrNames.size()) {
				for (i = 0; i < newAttrNames.size(); i ++) {
					attrId = newAttrIds.get(i);
					if (attrId != null)
						tableSchema.getAttrIdNameMap().put(attrId, newAttrNames.get(i));
					else
						logger.error(String.format("id for field:%s is null.", attrNames.get(i)));
				}
			} else {
				logger.error(String.format("id:%s and name:%s for table %s not matching.", newAttrIds, newAttrNames, name));
			}
			
			//generate alter table
			List<String> updateTableSqls = DBUtil.genUpdateTableSql(newAttrNames, newAttrTypes, name, 
					dbPrefix, getDbtype());
			
			//update/create create-table-sql
			logger.info(String.format("create/update table sqls are:%s", updateTableSqls));
			
			HdfsUtil.appendDfsFile(fs, this.createTablesSqlFileName, updateTableSqls);
			
			//update logic schema file
			if (schemaFile != null)
				safeWriteSchemaFile(defaultFs, schemaFile, tableSchema.isIndex(), tableSchema);
			
			//execute the sql
			if (dbtype != DBType.NONE){
				int result = DBUtil.executeSqls(updateTableSqls, super.getPc());
				//gen report info
				loginfo.add(result + ":" + updateTableSqls);
			}
			
			return tableSchema;
		}
	}
	
	private void updateSchemaIndex(LogicSchema logicSchema, String schemaFile) {
		LogicSchema index = new LogicSchema();
		schemaFile = schemaFile + (schemaFile.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR) + SchemaUtils.SCHEMA_INDEX_FILENAME;
		index.setIndex(true);
		index.setTableIdNameMap(logicSchema.getTableIdNameMap());
		safeWriteSchemaFile(defaultFs, schemaFile, false, index);
	}
	
	private final static class JVMLock extends ReentrantLock implements InterProcessLock {
		private static final long serialVersionUID = 1L;

		public void acquire() throws Exception {
			this.lock();
		}
		
		public boolean acquire(long time, TimeUnit unit) throws Exception {
			return this.tryLock(time, unit);
		}
		
		public boolean isAcquiredInThisProcess() {
			return true;
		}

		public void release() throws Exception {
			this.unlock();
		}
	}
	
	private final static class EmptyLock implements InterProcessLock {
		public void acquire() throws Exception {
		}
		
		public boolean acquire(long arg0, TimeUnit arg1) throws Exception {
			return true;
		}
		
		public boolean isAcquiredInThisProcess() {
			return true;
		}

		public void release() throws Exception {
		}
	}
	
	protected List<String> updateSchema(String id, String name, List<String> attrIds, List<String> attrNames, List<FieldType> attrTypes) throws Exception {
		List<String> loginfo = new ArrayList<String>();
		InterProcessLock lock;
		
		if (schemaFile != null && logicSchema.isIndex()) {
			String tableSchemaFile = schemaFile + (schemaFile.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR) + name + "." + SchemaUtils.SCHEMA_FILENAME_EXTENSION;
			
			if (LockType.zookeeper.equals(lockType))
				lock = new InterProcessMutex(client, tableSchemaFile);
			else if (LockType.jvm.equals(lockType))
				lock = new JVMLock();
			else
				lock = new EmptyLock();
			
			if (!lock.acquire(ZK_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS))
	        {
	            throw new IllegalStateException("Client could not acquire the lock");
	        } try {
	            LogicSchema tableSchema;
	            logger.debug("Client get the lock");
	            
				/* Reload the table schema */
	    		if (SchemaUtils.existsRemoteJsonPath(defaultFs, tableSchemaFile)) {
	    			tableSchema = SchemaUtils.fromRemoteJsonPath(defaultFs, tableSchemaFile, LogicSchema.class);
	    		} else {
	    			tableSchema = SchemaUtils.newRemoteInstance(defaultFs, tableSchemaFile);
					logger.debug(String.format("table schema file %s not exists, created new one.", tableSchemaFile));
	    		}
				
	    		if (!tableSchema.hasTable(name)) {
	    			//new table
	    			tableSchema = createTableSchema(tableSchema, tableSchemaFile, name, attrIds, attrNames, attrTypes, loginfo);
	    			
	    		} else {
	    			//update existing table
	    			tableSchema = updateTableSchema(tableSchema, tableSchemaFile, name, attrIds, attrNames, attrTypes, loginfo);
	    		}
				
	        } finally {
	        	logger.debug("Client releasing the lock");
	            lock.release(); // always release the lock in a finally block
	        }
		}

		if (LockType.zookeeper.equals(lockType))
			lock = new InterProcessMutex(client, schemaFile);
		else if (LockType.jvm.equals(lockType))
			lock = new JVMLock();
		else
			lock = new EmptyLock();
		
		if (!lock.acquire(ZK_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS))
        {
            throw new IllegalStateException("Client could not acquire the lock");
        } try {
            logger.debug("Client get the lock");
            
            /* Reload the schema */
    		if (schemaFile != null && SchemaUtils.existsRemoteJsonPath(defaultFs, schemaFile)) {
    			this.logicSchema = SchemaUtils.fromRemoteJsonPath(defaultFs, schemaFile, LogicSchema.class);
    		} else {
    			this.logicSchema = SchemaUtils.newRemoteInstance(defaultFs, schemaFile);
				logger.warn(String.format("schema file %s not exists.", schemaFile));
    		}
			this.getSystemVariables().put(VAR_LOGIC_SCHEMA, logicSchema);
			
			if (!logicSchema.hasTable(name)) {
				if (id != null) {
					/* Update the table id -> table name mapping */
					logicSchema.getTableIdNameMap().put(id, name);
				}else{
					logicSchema.getTableIdNameMap().put(name, name);
					logger.warn(String.format("the id for table:%s is null, use the name as id.", name));
				}
				
				if (logicSchema.isIndex())
					//update index file
					updateSchemaIndex(logicSchema, schemaFile);
				else
					//new table
					logicSchema = createTableSchema(logicSchema, schemaFile, name, attrIds, attrNames, attrTypes, loginfo);
				
			} else if (!logicSchema.isIndex()) {
	    		//update existing table
				logicSchema = updateTableSchema(logicSchema, schemaFile, name, attrIds, attrNames, attrTypes, loginfo);
			}
			
        } finally {
        	logger.debug("Client releasing the lock");
            lock.release(); // always release the lock in a finally block
        }
        
		return loginfo;
	}
	
	public List<String> updateSchema(Map<String, List<String>> attrNamesMap, Map<String, List<FieldType>> attrTypesMap){
		return updateSchema(null, null, attrNamesMap, attrTypesMap);
	}
	
	/**
	 * @param tIdToNameMap: table id to name map
	 * @param tnToattrIdsMap: table name to attr id list map
	 * @param attrNamesMap: table name to attr name list map
	 * @param attrTypesMap: table name to attr type list map
	 * @return
	 */
	public List<String> updateSchema(Map<String, String> tNameToIdMap, Map<String, List<String>> tnToattrIdsMap, 
			Map<String, List<String>> attrNamesMap, Map<String, List<FieldType>> attrTypesMap){
		List<String> loginfo = new ArrayList<String>();
		for (String newTable: attrNamesMap.keySet()){
			List<String> newAttrs = attrNamesMap.get(newTable);
			List<FieldType> newTypes = attrTypesMap.get(newTable);
			List<String> attrIds;
			String tid;
			
			if (tNameToIdMap!=null)
				/* Update the table id -> table name mapping */
				tid = tNameToIdMap.get(newTable);
			else
				tid = null;
			
			if (tnToattrIdsMap!=null)
				attrIds = tnToattrIdsMap.get(newTable);
			else
				attrIds = null;
			
			try {
				loginfo.addAll(
					updateSchema(tid, newTable, attrIds, newAttrs, newTypes)
				);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		return loginfo;
	}

	public DBType getDbtype() {
		return dbtype;
	}

	public void setDbtype(DBType dbtype) {
		this.dbtype = dbtype;
	}
	
	public List<String> getCreateSqls(){
		return SchemaUtils.genCreateSqlByLogicSchema(this.logicSchema, this.dbPrefix, this.dbtype);
	}
	
	public List<String> getDropSqls(){
		List<String> sqls = new ArrayList<String>();
		for (String tn: logicSchema.getAttrNameMap().keySet()){
			String sql = DBUtil.genDropTableSql(tn, dbPrefix);
			sqls.add(sql);
		}
		return sqls;
	}
	
	public String getTableName(String pathName){
		int lastSep = pathName.lastIndexOf("/");
		String fileName = pathName.substring(lastSep+1);
		this.getSystemVariables().put(VAR_NAME_FILE_NAME, fileName);
		String tableName = fileName;
		if (expFileTableMap!=null){
			tableName = ScriptEngineUtil.eval(expFileTableMap, this.getSystemVariables());
		}
		return tableName;
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
	
	public String getPathName(Mapper<LongWritable, Text, Text, Text>.Context context){
		if (context.getInputSplit() instanceof FileSplit){
			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			this.getSystemVariables().put(VAR_NAME_PATH_NAME, pathName);
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

	public String getStrFileTableMap() {
		return strFileTableMap;
	}

	public void setStrFileTableMap(String strFileTableMap) {
		this.strFileTableMap = strFileTableMap;
	}

	public OutputType getOutputType() {
		return outputType;
	}

	public void setOutputType(OutputType outputType) {
		this.outputType = outputType;
	}
}
