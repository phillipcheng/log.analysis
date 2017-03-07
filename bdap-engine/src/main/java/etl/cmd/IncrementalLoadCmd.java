package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import com.mchange.v2.c3p0.ComboPooledDataSource;

import etl.engine.ETLCmd;
import etl.engine.LogicSchema;
import etl.engine.types.MRMode;
import etl.engine.types.ProcessMode;
import etl.util.ConfigKey;
import etl.util.FieldType;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;
import scala.Tuple2;
import scala.Tuple3;

public class IncrementalLoadCmd extends SchemaETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(IncrementalLoadCmd.class);
	
//	private static volatile ComboPooledDataSource ds = null;
	
	//cfgkey
	public static final @ConfigKey String cfgkey_tables="tables";
	public static final @ConfigKey(type=String[].class) String cfgkey_primary_keys="primary.keys";
	public static final @ConfigKey String cfgkey_table_key="table.key";
	public static final @ConfigKey String cfgkey_operation_key="operation.key";
	public static final @ConfigKey String cfgkey_event_timestamp_key="event.timestamp.key";
	public static final @ConfigKey String cfgkey_table_set="set"; //table field set operation
	public static final @ConfigKey String cfgkey_table_set_when_exist="setWhenExist";//table field set operation if condition meet.
	
//	public static final @ConfigKey String cfgkey_updateDB="updateDB";
//	public static final @ConfigKey String cfgkey_throttle="throttle";
//	
//	public static final @ConfigKey String cfgkey_driver_class="driver.class";
//	public static final @ConfigKey String cfgkey_jdbc_url="jdbc.url";
//	public static final @ConfigKey String cfgkey_user="user";
//	public static final @ConfigKey String cfgkey_password="password";
//	public static final @ConfigKey String cfgkey_acquireIncrement="acquireIncrement";
//	public static final @ConfigKey String cfgkey_initialPoolSize="initialPoolSize";
//	public static final @ConfigKey String cfgkey_minPoolSize="minPoolSize";
//	public static final @ConfigKey String cfgkey_maxPoolSize="maxPoolSize";
	
//	private boolean updateDB;
//	private int maxThrottle;
	private String tableKey;
	private String operationKey;
	private String eventTimestampKey;
	private String[] tables;
	private Map<String,TableOpConfig> tableOpConfigMap=new HashMap<String,TableOpConfig>();
	
	public IncrementalLoadCmd(){
		super();
	}
	
	public IncrementalLoadCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public IncrementalLoadCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public IncrementalLoadCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
//	public static AtomicLong counter=new AtomicLong(0);
//	public static ThreadLocal<Connection> connTL=new ThreadLocal<Connection>();
//	public static void main(String[] args){
//		ds = new ComboPooledDataSource();
//		try {
//			ds.setDriverClass("com.vertica.jdbc.Driver");
//			ds.setJdbcUrl("jdbc:vertica://192.85.247.104:5433/cmslab");
//			ds.setUser("dbadmin");
//			ds.setPassword("password");
//			ds.setAcquireIncrement(1);
//			ds.setInitialPoolSize(10);
//			ds.setMinPoolSize(10);
//			ds.setMaxPoolSize(10);
////			ds.setAutoCommitOnClose(false);
//		} catch (PropertyVetoException e) {
//			logger.error("Initial datasource failed", e);
//		}
//			
//		ExecutorService es=Executors.newFixedThreadPool(10);
//	    long start=System.currentTimeMillis();
//	    System.out.println("start time2:"+start);
//		long c=1000l;
//		for(long i=0;i<c;i++){
//			es.submit(new Runnable(){
//				public void run(){
//					Connection conn=connTL.get();
//				    try {
//				    	if(conn==null){
//				    		conn=ds.getConnection();
//				    		connTL.set(conn);
//				    	}
//						
//						Statement stat=conn.createStatement();
//						for(int j=0;j<10;j++){
//							stat.addBatch("Insert into spc.test values('sv','2012-10-10 10:10:00', '2012-10-10', 1000,30 )");
//						}						
//						stat.executeBatch();
//						long cval=counter.incrementAndGet();
//						if(cval%100==0){
//							System.out.println(String.format("%s:executed %s", System.currentTimeMillis()-start, cval));
//						}
////						conn.commit();
////						conn.close();
//					} catch (SQLException e) {
//						e.printStackTrace();
//						if(conn!=null){
//							try {
//								conn.close();
//								connTL.remove();
//							} catch (SQLException e1) {
//								// TODO Auto-generated catch block
//								e1.printStackTrace();
//							}
//						}
//					}			    
//				}
//				
//			});
//		}
//		long end=System.currentTimeMillis();
//		
//		System.out.println(end-start);
//	}
//	public static void main2(String[] args){
//		ds = new ComboPooledDataSource();
//		try {
//			ds.setDriverClass("com.vertica.jdbc.Driver");
//			ds.setJdbcUrl("jdbc:vertica://192.85.247.104:5433/cmslab");
//			ds.setUser("dbadmin");
//			ds.setPassword("password");
//			ds.setAcquireIncrement(1);
//			ds.setInitialPoolSize(10);
//			ds.setMinPoolSize(10);
//			ds.setMaxPoolSize(10);
////			ds.setAutoCommitOnClose(false);
//		} catch (PropertyVetoException e) {
//			logger.error("Initial datasource failed", e);
//		}
//			
//		ExecutorService es=Executors.newFixedThreadPool(10);
//	    long start=System.currentTimeMillis();
//	    System.out.println("start time:"+start);
//		long c=1000l;
//		for(long i=0;i<c;i++){
//			es.submit(new Runnable(){
//				public void run(){
//					Connection conn=null;
//				    try {
//						conn=ds.getConnection();
//						conn.setAutoCommit(false);
//						Statement stat=conn.createStatement();
//						for(int j=0;j<10;j++){
//							stat.addBatch("Insert into spc.test values('sv','2012-10-10 10:10:00', '2012-10-10', 1000,30 )");
//						}						
//						stat.executeBatch();
//						long cval=counter.incrementAndGet();
//						if(cval%100==0){
//							System.out.println(String.format("%s:executed %s", System.currentTimeMillis()-start, cval));
//						}
////						conn.commit();
//						conn.close();
//					} catch (SQLException e) {
//						e.printStackTrace();
//						if(conn!=null){
//							try {
//								conn.close();
//							} catch (SQLException e1) {
//								// TODO Auto-generated catch block
//								e1.printStackTrace();
//							}
//						}
//					}			    
//				}
//				
//			});
//		}
//		long end=System.currentTimeMillis();
//		
//		System.out.println(end-start);
//	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		logger.info("--------------------------------------------Start initial--------------------------------------------");		
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		this.setMrMode(MRMode.line);
		
		tableKey=super.getCfgString(cfgkey_table_key, null);
		operationKey=super.getCfgString(cfgkey_operation_key, null);
		eventTimestampKey=super.getCfgString(cfgkey_event_timestamp_key, null);
		tables=super.getCfgStringArray(cfgkey_tables);
		if(tables==null) tables=new String[0];
		tableOpConfigMap=initTableOpConfig();
		
//		updateDB=super.getCfgBoolean(cfgkey_updateDB, false);
//		maxThrottle=super.getCfgInt(cfgkey_throttle, 10);
//		
//		String driverClass=super.getCfgString(cfgkey_driver_class, null);
//		String jdbcUrl=super.getCfgString(cfgkey_jdbc_url, null);
//		String user=super.getCfgString(cfgkey_user, null);
//		String password=super.getCfgString(cfgkey_password, null);
//		int acquireIncrement=super.getCfgInt(cfgkey_acquireIncrement, 1);
//		int initialPoolSize=super.getCfgInt(cfgkey_initialPoolSize, 10);
//		int minPoolSize=super.getCfgInt(cfgkey_minPoolSize, 10);
//		int maxPoolSize=super.getCfgInt(cfgkey_maxPoolSize, 10);
//		
//		if(updateDB==true){
//			synchronized(this.getClass()){
//				if (ds==null){
//					ds=new ComboPooledDataSource();
//					try {
//						ds.setDriverClass(driverClass);
//						ds.setJdbcUrl(jdbcUrl);
//						ds.setUser(user);
//						ds.setPassword(password);
//						ds.setAcquireIncrement(acquireIncrement);
//						ds.setInitialPoolSize(initialPoolSize);
//						ds.setMinPoolSize(minPoolSize);
//						ds.setMaxPoolSize(maxPoolSize);
//					} catch (PropertyVetoException e) {
//						logger.error("Initial datasource failed",e);
//					}
//				}
//			}
//		}		
	}
	
	public static Map<String, String> parseLog(String log) throws Exception{
		Map<String,String> result=new HashMap<String,String>();
		
		if(log==null || log.isEmpty()) return result;		
		
		char[] chars=log.toCharArray();
		StringBuilder sb=new StringBuilder();
		String key=null;
		String value=null;
		boolean escapeChar=false;
		for(int i=0;i<chars.length;i++){
			char currChar=chars[i];
			if(currChar=='\\'){
				if(escapeChar==true){
					sb.append(currChar);
					escapeChar=false;
				}else{
					escapeChar=true;
				}
			}else if(currChar=='='){
				if(i==0){
					throw new Exception("Invalid string");
				}
				if(escapeChar==true){
					sb.append(currChar);
					escapeChar=false;
				}else{
					key=sb.toString();
					sb.setLength(0);
				}
			}else if(currChar==','){
				if(i==0){
					throw new Exception("Invalid string");
				}
				if(escapeChar==true){
					sb.append(currChar);
					escapeChar=false;
				}else{
					value=sb.toString();
					sb.setLength(0);
					
					if(key==null) {
						throw new Exception("Invalid string");
					}else{
						result.put(key.trim(), value.trim());
						key=null;
						value=null;
					}
				}
			}else{
				sb.append(currChar);
			}
		}
		
		if(escapeChar==true){
			throw new Exception("Invalid string");
		}
		
		if(key!=null){
			value=sb.toString();
			result.put(key.trim(), value.trim());
		}
		
		return result;
	}
	
	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tfName, String value, Mapper<LongWritable, Text, Text, Text>.Context context){
		super.init();
		List<Tuple2<String, String>> result=new ArrayList<Tuple2<String, String>>();
		
		//Parse the raw data
		Map<String,String> record=null;
		try {
			record=parseLog(value);
		} catch (Exception e) {
			logger.warn("Incorrect value:{}", value);
			return null;
		}
		
		String table=record.get(tableKey);
		
		TableOpConfig tableOpConfig=tableOpConfigMap.get(table);
		//check table whether correct
		if(tableOpConfig==null || !getLogicSchema().hasTable(table)){
			logger.warn("Drop the record as releated table is not defined in action or Logica Schema:{}", value);
			return result;
		}
		
		//execute field op
		List<FieldOp> fieldOps=tableOpConfig.getFieldOps();
		for(FieldOp fieldOp:fieldOps){			
			fieldOp.execute(record, this.getSystemVariables());
		}
		
		//get primary keys value
		String[] primaryKeys=tableOpConfig.getPrimaryKeys();
		if(primaryKeys==null || primaryKeys.length==0){
			logger.warn("Drop the record as primaryKeys are not defined:{}", value);
			return result;
		}
		
		List<String> primaryKeyValues=new ArrayList<String>();
		for(String primaryKey:primaryKeys){
			String keyValue=record.get(primaryKey);
			if(keyValue==null){
				logger.warn("Drop the record:{} as missing primary key:{}", value, primaryKey);
				return result;
			}else{
				primaryKeyValues.add(keyValue);
			}				
		}
		
		//rebuild result
		StringBuilder sb=new StringBuilder();
		for(String key:record.keySet()){
			String keyValue=record.get(key);
			keyValue=keyValue.replace("\\", "\\\\").replace("=", "\\=").replace(",", "\\,");
			sb.append(key).append("=").append(keyValue).append(",");
		}
		sb.setLength(sb.length()-1);
		
		result.add(new Tuple2<String, String>(table+","+String.join(",", primaryKeyValues), sb.toString()));

		
		//Output:
		//Key: tableName, keys
		//Value: records		
		return result;
	}
	
	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<? extends Object> values,
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos){		
		/*
		 * According eventTimeStamp sort record(values)
		 * execute each record operation to get final result
		 * according to final operation type and final value to generate sql and execute it
		 */
		
		List<Tuple3<String, String, String>> ret =new ArrayList<Tuple3<String, String, String>>();
		
		//Parse record
		List<Map<String,String>> records = new ArrayList<Map<String,String>>();
		for(Object obj:values){
			String value = obj.toString();
			try {
				records.add(parseLog(value));
			} catch (Exception e) {
				logger.warn("Invalid record found, cancel all operation for same key",e);
				return ret;
			}
		}
		
		String table=key.substring(0, key.indexOf(','));
		
		//Sort record
		records.sort(new Comparator<Map<String,String>>(){
			@Override
			public int compare(Map<String, String> o1, Map<String, String> o2) {
				long eventTimeStamp1=Long.parseLong(o1.get(eventTimestampKey));
				long eventTimeStamp2=Long.parseLong(o2.get(eventTimestampKey));
				long result=eventTimeStamp1-eventTimeStamp2;
				if(result==0) return 0;
				if(result>0) return 1;
				return -1;
			}
		});
		
		//Generate SQL
		for(Map<String, String> record: records){
			String operation=record.get(operationKey);
			operation=operation.toUpperCase();
			String sql=generateSQL(table,operation, record);
			ret.add(new Tuple3<String,String,String>(sql,null, ETLCmd.SINGLE_TABLE));
		}
		
//		//Execute SQL
//		if(updateDB){			
//			for(Tuple3<String,String,String> item:ret){
//				String sql=item._1();
//				Connection conn=null;
//				try {
//					conn=ds.getConnection();
//					conn.setAutoCommit(true);
//					Statement stat=conn.createStatement();
//					stat.execute(sql);
//					conn.close();
//				} catch (SQLException e) {
//					logger.warn(String.format("Failed to execute SQL:%s",sql), e);
//					if(conn!=null){
//						try {
//							conn.close();
//						} catch (SQLException e1) {
//							logger.warn("", e1);
//						}
//					}
//					break;						
//				}
//			}
//		}
		
		return ret;
	}
	
	private String generateSQL(String table, String operation, Map<String,String> record){
		LogicSchema ls=getLogicSchema();
		List<String> attrtNames=ls.getAttrNames(table);
		List<FieldType> attrTypes=ls.getAttrTypes(table);
		TableOpConfig tableOpConfig=getTableOpConfig(table);
		List<String> configPrimaryKeys=Arrays.asList(tableOpConfig.getPrimaryKeys());
			
		operation=operation.toUpperCase();
		List<String> nonPrimaryKeys=new ArrayList<String>();
		List<String> nonPrimaryKeyValues=new ArrayList<String>();
		List<String> primaryKeys=new ArrayList<String>();
		List<String> primaryKeyValues=new ArrayList<String>();
		List<String> allKeys=new ArrayList<String>();
		List<String> allKeyValues=new ArrayList<String>();
		
		for(String key:record.keySet()){
			int idx=attrtNames.indexOf(key);
			if(idx==-1) continue;
			String value=record.get(key);
			FieldType fieldType=attrTypes.get(idx);
			
			List<String> keys;
			List<String> keyValues;
			if(configPrimaryKeys.contains(key)){
				keys=primaryKeys;
				keyValues=primaryKeyValues;
			}else{
				keys=nonPrimaryKeys;
				keyValues=nonPrimaryKeyValues;
			}
			
			keys.add(key);
			
			VarType varType=fieldType.getType();
			if(VarType.NUMERIC==varType || VarType.INT==varType || VarType.FLOAT==varType){
				keyValues.add(value);				
			}else{
				keyValues.add(String.format("'%s'", value.replaceAll("'", "''")));
			}			
		}
		
		allKeys.addAll(primaryKeys);
		allKeys.addAll(nonPrimaryKeys);
		
		allKeyValues.addAll(primaryKeyValues);
		allKeyValues.addAll(nonPrimaryKeyValues);
		
		
		if("INSERT".equals(operation)){
			StringBuilder sqlSB=new StringBuilder();			
			
			String colsStr=String.join(",", allKeys);
			String valuesStr=String.join(",", allKeyValues);			
			
			sqlSB.append("INSERT INTO ").append(dbPrefix).append(".").append(table).append(" (").append(colsStr).append(" ) VALUES ( ").append(valuesStr).append( ");");
			return sqlSB.toString();
		}else if("UPDATE".equals(operation)){
			StringBuilder sqlSB=new StringBuilder();
			sqlSB.append("UPDATE ").append(dbPrefix).append(".").append(table).append(" SET ");
			for(int i=0;i<nonPrimaryKeys.size();i++){
				sqlSB.append(nonPrimaryKeys.get(i)).append("=").append(nonPrimaryKeyValues.get(i)).append(",");
			}
			sqlSB.setLength(sqlSB.length()-1);
			sqlSB.append(" WHERE ");
			for(int i=0;i<primaryKeys.size();i++){
				sqlSB.append(primaryKeys.get(i)).append("=").append(primaryKeyValues.get(i)).append(" AND ");
			}
			sqlSB.setLength(sqlSB.length()-5);
			sqlSB.append(";");
			return sqlSB.toString();
		}else if("DELETE".equals(operation)){
			StringBuilder sqlSB=new StringBuilder();
			sqlSB.append("DELETE FROM ").append(dbPrefix).append(".").append(table).append(" WHERE ");
			for(int i=0;i<allKeys.size();i++){
				sqlSB.append(allKeys.get(i)).append("=").append(allKeyValues.get(i)).append(" AND ");
			}
			sqlSB.setLength(sqlSB.length()-5);
			sqlSB.append(";");
			return sqlSB.toString();
		}
		
		return null;
	}
	
	public Map<String,TableOpConfig> initTableOpConfig(){
		Map<String,TableOpConfig> tableOpConfigMap=new HashMap<String,TableOpConfig>();
		for(String table:tables){
			TableOpConfig tableOpConfig=getTableOpConfig(table);
			tableOpConfigMap.put(table, tableOpConfig);
		}		
		return tableOpConfigMap;
	}
	
	private TableOpConfig getTableOpConfig(String tablename){
		TableOpConfig tableOpConfig=tableOpConfigMap.get(tablename);
		if(tableOpConfig!=null) return tableOpConfig;
		
		//Read TableOpConfig
		String[] primaryKeys=super.getCfgStringArray(tablename+"."+cfgkey_primary_keys);
		String operationKey=super.getCfgString(tablename+"."+cfgkey_operation_key, this.operationKey);
		String eventTimestampKey=super.getCfgString(tablename+"."+cfgkey_event_timestamp_key, this.eventTimestampKey);
		String[] opSet=super.getCfgStringArray(tablename+"."+cfgkey_table_set);
		String[] opSetWhenExist=super.getCfgStringArray(tablename+"."+cfgkey_table_set_when_exist);
		List<FieldOp> fieldOps=new ArrayList<FieldOp>();
		if(opSet!=null && opSet.length>0){
			for(String op:opSet){
				fieldOps.add(new FieldOp(FieldOp.OP_TYPE_SET,op));
			}
		}
		if(opSetWhenExist!=null && opSetWhenExist.length>0){
			for(String op:opSetWhenExist){
				fieldOps.add(new FieldOp(FieldOp.OP_TYPE_SET_WHEN_EXIST,op));
			}
		}
		
		tableOpConfig=new TableOpConfig(operationKey,eventTimestampKey,primaryKeys, fieldOps);
		return tableOpConfig;
	}
	
	
	public class TableOpConfig{
		private String operationKey;
		private String eventTimestampKey;
		private String[] primaryKeys;
		private List<FieldOp> fieldOps;
		
		public TableOpConfig(String operationKey, String eventTimestampKey, String[] primaryKeys, List<FieldOp> fieldOps) {
			super();
			this.operationKey = operationKey;
			this.eventTimestampKey = eventTimestampKey;
			this.primaryKeys = primaryKeys;
			this.fieldOps=fieldOps;
		}
		public String getOperationKey() {
			return operationKey;
		}
		public void setOperationKey(String operationKey) {
			this.operationKey = operationKey;
		}
		public String getEventTimestampKey() {
			return eventTimestampKey;
		}
		public void setEventTimestampKey(String eventTimestampKey) {
			this.eventTimestampKey = eventTimestampKey;
		}
		public String[] getPrimaryKeys() {
			return primaryKeys;
		}
		public void setPrimaryKeys(String[] primaryKeys) {
			this.primaryKeys = primaryKeys;
		}
		public List<FieldOp> getFieldOps() {
			return fieldOps;
		}
		public void setFieldOps(List<FieldOp> fieldOps) {
			this.fieldOps = fieldOps;
		}	
		
	}
	
	public class FieldOp{
		public static final int OP_TYPE_SET=1;
		public static final int OP_TYPE_SET_WHEN_EXIST=2;
		
		private int opType;
		private String fieldName;
		private String checkFieldName;
		private CompiledScript valCS;
		
		public void execute(Map<String,String> record, Map<String,Object> variables){
			if(opType==OP_TYPE_SET){
				Map<String, Object> allVars=new HashMap<String, Object>();
				allVars.putAll(variables);
				allVars.put("record", record);
				String value=ScriptEngineUtil.eval(valCS, allVars);
				record.put(fieldName, value);
			}else if(opType==OP_TYPE_SET_WHEN_EXIST){
				if(record.containsKey(checkFieldName)){
					Map<String, Object> allVars=new HashMap<String, Object>();
					allVars.putAll(variables);
					allVars.put("record", record);
					String value=ScriptEngineUtil.eval(valCS, allVars);
					record.put(fieldName, value);
				}
			}
		}
		
		public FieldOp(int opType, String opConfig) {
			super();
			this.opType=opType;
			if(opType==OP_TYPE_SET){
				int idx=opConfig.indexOf(":");
				fieldName=opConfig.substring(0,idx);
				String opExp=opConfig.substring(idx+1, opConfig.length());
				valCS = ScriptEngineUtil.compileScript(opExp);
			}else if(opType==OP_TYPE_SET_WHEN_EXIST){
				int idx=opConfig.indexOf(':');
				int idx2=opConfig.indexOf(':', idx+1);
				fieldName=opConfig.substring(0,idx);
				checkFieldName=opConfig.substring(idx+1,idx2);
				String opExp=opConfig.substring(idx2+1, opConfig.length());
				valCS = ScriptEngineUtil.compileScript(opExp);
			}			
		}
		
		public int getOpType() {
			return opType;
		}
		public void setOpType(int opType) {
			this.opType = opType;
		}
		public String getFieldName() {
			return fieldName;
		}
		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}

		public String getCheckFieldName() {
			return checkFieldName;
		}

		public void setCheckFieldName(String checkFieldName) {
			this.checkFieldName = checkFieldName;
		}

		public CompiledScript getValCS() {
			return valCS;
		}

		public void setValCS(CompiledScript valCS) {
			this.valCS = valCS;
		}
			 
	}
	
}
