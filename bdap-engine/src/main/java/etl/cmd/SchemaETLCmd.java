package etl.cmd;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import javax.script.CompiledScript;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import etl.engine.ETLCmd;
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
import etl.zookeeper.lock.WriteLock;

public abstract class SchemaETLCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LogManager.getLogger(SchemaETLCmd.class);

	//cfgkey
	public static final String cfgkey_schema_file="schema.file";
	public static final String cfgkey_file_table_map="file.table.map";
	public static final String cfgkey_create_sql="create.sql";
	public static final String cfgkey_db_prefix="db.prefix"; //db schema
	public static final String cfgkey_db_type="db.type";
	public static final String cfgkey_output_type="output.type";
	
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
		if (loadSchema){
			loadSchema(null);
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
	
	protected void loadSchema(String zkUrl){
		logger.info(String.format("start load schemaFile: %s", schemaFile));
		if (zkUrl!=null){
			WriteLock lock=null;
			boolean finished=false;
			while(!finished){
				try {
					lock = this.getZookeeperLock(zkUrl);
					if (lock.lock()){
						while (!lock.isOwner()){
							Thread.sleep(1000);
						}
						fetchLogicSchema();
						finished=true;
					}else{
						Thread.sleep(1000);
					}
				}catch(Exception e){
					logger.error("", e);
				}finally{
					try {
						this.releaseLock(lock);
					}catch(Exception e){
						logger.error("", e);
					}
				}
			}
		}else{
			fetchLogicSchema();
		}
		logger.info(String.format("end load schemaFile: %s", schemaFile));
	}
	
	public static class CountdownWatcher implements Watcher {
        // XXX this doesn't need to be volatile! (Should probably be final)
        volatile CountDownLatch clientConnected;
        volatile boolean connected;

        public CountdownWatcher() {
            reset();
        }
        synchronized public void reset() {
            clientConnected = new CountDownLatch(1);
            connected = false;
        }
        synchronized public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected ||
                event.getState() == KeeperState.ConnectedReadOnly) {
                connected = true;
                notifyAll();
                clientConnected.countDown();
            } else {
                connected = false;
                notifyAll();
            }
        }
        synchronized public boolean isConnected() {
            return connected;
        }
        synchronized public void waitForConnected(long timeout)
            throws InterruptedException, TimeoutException
        {
            long expire = System.currentTimeMillis() + timeout;
            long left = timeout;
            while(!connected && left > 0) {
                wait(left);
                left = expire - System.currentTimeMillis();
            }
            if (!connected) {
                throw new TimeoutException("Did not connect");

            }
        }
        synchronized public void waitForDisconnected(long timeout)
            throws InterruptedException, TimeoutException
        {
            long expire = System.currentTimeMillis() + timeout;
            long left = timeout;
            while(connected && left > 0) {
                wait(left);
                left = expire - System.currentTimeMillis();
            }
            if (connected) {
                throw new TimeoutException("Did not disconnect");

            }
        }
    }
	
	protected WriteLock getZookeeperLock(String zookeeperUrl) throws Exception {
		CountdownWatcher watcher = new CountdownWatcher();
		ZooKeeper zk = new ZooKeeper(zookeeperUrl, ZK_CONNECTION_TIMEOUT, watcher);
		WriteLock lock = new WriteLock(zk, this.schemaFile, null);
		return lock;
	}
	
	protected void releaseLock(WriteLock lock) throws Exception{
		if (lock!=null){
			lock.unlock();
			lock.close();
			lock.getZookeeper().close();
		}
	}
	
	public void fetchLogicSchema(){
		if (this.schemaFile!=null){
			try{
				Path schemaFilePath = new Path(schemaFile);
				if (fs.exists(schemaFilePath)){
					schemaFileName = schemaFilePath.getName();
					this.logicSchema = (LogicSchema) HdfsUtil.fromDfsJsonFile(fs, schemaFile, LogicSchema.class);
				}else{
					this.logicSchema = new LogicSchema();
					logger.warn(String.format("schema file %s not exists.", schemaFile));
				}
				this.getSystemVariables().put(VAR_LOGIC_SCHEMA, logicSchema);
			}catch(Exception e){
				logger.error("", e);
			}
		}
	}
	
	@Override
	public VarDef[] getCfgVar(){
		return (VarDef[]) ArrayUtils.addAll(super.getCfgVar(), new VarDef[]{});
	}
	
	@Override
	public VarDef[] getSysVar(){
		return (VarDef[]) ArrayUtils.addAll(super.getSysVar(), new VarDef[]{});
	}
	
	//return loginfo
	private List<String> flushLogicSchemaAndSql(List<String> createTableSqls){
		if (createTableSqls.size()>0){
			//update/create create-table-sql
			logger.info(String.format("create/update table sqls are:%s", createTableSqls));
			HdfsUtil.appendDfsFile(fs, this.createTablesSqlFileName, createTableSqls);
			//update logic schema file
			HdfsUtil.toDfsJsonFile(fs, this.schemaFile, logicSchema);
			//execute the sql
			if (dbtype != DBType.NONE){
				DBUtil.executeSqls(createTableSqls, super.getPc());
			}
		}
		//gen report info		
		List<String> loginfo = new ArrayList<String>();
		loginfo.add(createTableSqls.size()+"");
		return loginfo;
	}
	
	public void genSchemaSql(Map<String, List<String>> attrsMap, Map<String, List<FieldType>> attrTypesMap, String schemaFile, String sqlFile){
		List<String> createTableSqls = new ArrayList<String>();
		LogicSchema newls = new LogicSchema();
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
		boolean schemaUpdated = false;
		List<String> createTableSqls = new ArrayList<String>();
		for (String newTable: attrNamesMap.keySet()){
			List<String> newAttrs = attrNamesMap.get(newTable);
			List<FieldType> newTypes = attrTypesMap.get(newTable);
			if (tnToattrIdsMap!=null){
				List<String> attrIds = tnToattrIdsMap.get(newTable);
				if (attrIds!=null && attrIds.size()==newAttrs.size()){
					for (int i=0; i<attrIds.size(); i++){
						String attrId = attrIds.get(i);
						if (attrId!=null){
							logicSchema.getAttrIdNameMap().put(attrIds.get(i), newAttrs.get(i));
						}else{
							logger.error(String.format("id for field:%s is null.", newAttrs.get(i)));
						}
					}
				}else{
					logger.error(String.format("id:%s and name:%s for table %s not matching.", attrIds, newAttrs, newTable));
				}
			}
			if (tNameToIdMap!=null){
				String tid = tNameToIdMap.get(newTable);
				if (tid!=null){
					logicSchema.getTableIdNameMap().put(tid, newTable);
				}else{
					logger.error(String.format("the id for table:%s is null.", newTable));
				}
			}
			if (!logicSchema.hasTable(newTable)){//new table
				logicSchema.updateTableAttrs(newTable, newAttrs);
				logicSchema.updateTableAttrTypes(newTable, newTypes);
				schemaUpdated = true;
				//generate create table
				createTableSqls.add(DBUtil.genCreateTableSql(newAttrs, newTypes, newTable, 
						dbPrefix, getDbtype()));
			}else{//update existing table
				List<String> existNewAttrs = logicSchema.getAttrNames(newTable);
				if (existNewAttrs.containsAll(newAttrs)){//
					logger.error(String.format("update nothing for %s", newTable));
				}else{
					//update schema, happens only when the schema is updated by external force
					logicSchema.addAttributes(newTable, newAttrs);
					logicSchema.addAttrTypes(newTable, newTypes);
					schemaUpdated = true;
					//generate alter table
					createTableSqls.addAll(DBUtil.genUpdateTableSql(newAttrs, newTypes, newTable, 
							dbPrefix, getDbtype()));
				}
			}
		}
		if (schemaUpdated){
			return flushLogicSchemaAndSql(createTableSqls);
		}else{
			return null;
		}
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
		String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
		this.getSystemVariables().put(VAR_NAME_PATH_NAME, pathName);
		return pathName;
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
