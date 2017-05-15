package etl.cmd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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

import bdap.util.Util;
import etl.engine.ETLCmd;
import etl.engine.EngineUtil;
import etl.engine.LogicSchema;
import etl.engine.types.MRMode;
import etl.engine.types.OutputType;
import etl.engine.types.ProcessMode;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;
import scala.Tuple3;

public class MapToCsvCmd extends SchemaETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(MapToCsvCmd.class);
	
	//cfgkey
	public static final @ConfigKey String cfgkey_tables="tables";
	public static final @ConfigKey String cfgkey_table_key="table.key";
	public static final @ConfigKey String cfgkey_table_set="set"; //table field set operation
	public static final @ConfigKey String cfgkey_table_set_when_exist="setWhenExist";//table field set operation if condition meet.
	public static final @ConfigKey String cfgkey_default_value="default.value"; 
	public static final @ConfigKey String cfgkey_table_mapping="tablename.mapping.exp";
	public static final @ConfigKey(type=Boolean.class) String cfgkey_setfilename_mapping = "tablename.setfilename";
	
	public static final String VAR_NAME_ORIGIN_TABLE_NAME="originTableName";
	
	private String tableKey;
	private String[] tables;
	private String defaultValue;
	private Boolean tablenametoFilename;
	private transient CompiledScript tableMappingCS=null;
	private transient Map<String,TableOpConfig> tableOpConfigMap=null;
	
	public MapToCsvCmd(){
		super();
	}
	
	public MapToCsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public MapToCsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public MapToCsvCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		logger.info("--------------------------------------------Start initial--------------------------------------------");		
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		this.setMrMode(MRMode.line);
		
		tableKey=super.getCfgString(cfgkey_table_key, null);
		tables=super.getCfgStringArray(cfgkey_tables);
		if(tables==null) tables=new String[0];
		initTableOpConfig();
		defaultValue = super.getCfgString(cfgkey_default_value, null);
		//Read table name mapping expression
		String tableNameMappingExp=this.getCfgString(cfgkey_table_mapping, null);
		if(tableNameMappingExp!=null){
			tableMappingCS=ScriptEngineUtil.compileScript(tableNameMappingExp);
		}
		tablenametoFilename = super.getCfgBoolean(cfgkey_setfilename_mapping, false);
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
		getSystemVariables().put(VAR_NAME_ORIGIN_TABLE_NAME, table);
		if(tableMappingCS!=null){
			table=ScriptEngineUtil.eval(tableMappingCS, super.getSystemVariables());
		}
		
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
		
		// get AttrNames list
		LogicSchema ls=getLogicSchema();
		List<String> attrtNames=ls.getAttrNames(table);
		if(defaultValue == null || defaultValue.isEmpty()){
			defaultValue = "";
		}
		
		List<String> listValues = new ArrayList<String>();
		for (String attrtName : attrtNames) {
			if(record.containsKey(attrtName)){
				listValues.add(record.get(attrtName));
			}else{
				listValues.add(defaultValue);
			}
		}
		logger.debug(listValues);
		String output = Util.getCsv(listValues, ",", true, false);
		return Arrays.asList(new Tuple2<String, String>(tfName, output));
	}
	
	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<? extends Object> values,
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{		
		String pathName = key.toString();
		int lastSep = pathName.lastIndexOf("/");
		String fileName = pathName.substring(lastSep+1);
		Iterator<? extends Object> it = values.iterator();
		List<Tuple3<String, String, String>> ret = new ArrayList<Tuple3<String, String, String>>();
		while (it.hasNext()){
			String v = it.next().toString();
			String tableName = ETLCmd.SINGLE_TABLE;
			if (super.getOutputType()==OutputType.multiple){
				if(tablenametoFilename && v!=null){
					tableName = v.split(",")[0];
				}else{
					tableName = fileName.toString();
				}
				
			}
			if (context!=null){//map reduce
				EngineUtil.processReduceKeyValue(v, null, tableName, context, mos);
			}else{
				ret.add(new Tuple3<String, String, String>(v, null, tableName));
			}
		}
		return ret;
	}
	
	public Map<String,TableOpConfig> initTableOpConfig(){
		tableOpConfigMap=new HashMap<String,TableOpConfig>();
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
		String[] opSet=super.getCfgStringArray(tablename+"."+cfgkey_table_set);
		String[] opSetWhenExist=super.getCfgStringArray(tablename+"."+cfgkey_table_set_when_exist);
		String[] commonOpSetWhenExist=super.getCfgStringArray("table.common."+cfgkey_table_set_when_exist);
		
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
		
		if(commonOpSetWhenExist!=null && commonOpSetWhenExist.length>0){
			for(String op:commonOpSetWhenExist){
				fieldOps.add(new FieldOp(FieldOp.OP_TYPE_SET_WHEN_EXIST,op));
			}
		}
		
		tableOpConfig=new TableOpConfig(fieldOps);
		return tableOpConfig;
	}
	
	
	public class TableOpConfig implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private List<FieldOp> fieldOps;
		
		public TableOpConfig(List<FieldOp> fieldOps) {
			super();
			this.fieldOps=fieldOps;
		}
		public List<FieldOp> getFieldOps() {
			return fieldOps;
		}
		public void setFieldOps(List<FieldOp> fieldOps) {
			this.fieldOps = fieldOps;
		}	
		
	}
	
	public class FieldOp implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
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
				String value=(String)ScriptEngineUtil.evalObject(valCS, allVars);
				record.put(fieldName, value);
			}else if(opType==OP_TYPE_SET_WHEN_EXIST){
				if(record.containsKey(checkFieldName)){
					Map<String, Object> allVars=new HashMap<String, Object>();
					allVars.putAll(variables);
					allVars.put("record", record);
					String value=(String)ScriptEngineUtil.evalObject(valCS, allVars);
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
