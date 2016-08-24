package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.script.CompiledScript;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import etl.cmd.transform.AggrOp;
import etl.cmd.transform.AggrOpMap;
import etl.cmd.transform.GroupOp;
import etl.engine.AggrOperator;
import etl.engine.ETLCmd;
import etl.engine.FileETLCmd;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.IdxRange;
import etl.util.ScriptEngineUtil;
import etl.util.Util;

public class CsvAggregateCmd extends DynaSchemaFileETLCmd{
	public static final Logger logger = Logger.getLogger(CsvAggregateCmd.class);

	public static final String AGGR_OP_SUM="sum";
	public static final String AGGR_OPERATOR_SEP="\\|";
	
	public static final String cfgkey_aggr_op="aggr.op";
	public static final String cfgkey_aggr_groupkey="aggr.groupkey";
	public static final String cfgkey_aggr_groupkey_exp="aggr.groupkey.exp";
	public static final String cfgkey_aggr_groupkey_exp_type="aggr.groupkey.exp.type";
	public static final String cfgkey_aggr_groupkey_exp_name="aggr.groupkey.exp.name";
	
	public static final String cfgkey_aggr_old_table="old.table";
	public static final String cfgkey_aggr_new_table="new.table";
	public static final String cfgkey_file_table_map="file.table.map";
	
	private String[] oldTables = null;
	private String[] newTables = null;
	private Map<String, AggrOpMap> aoMapMap = new HashMap<String, AggrOpMap>(); //table name to AggrOpMap
	private Map<String, GroupOp> groupKeysMap = new HashMap<String, GroupOp>(); //table name to 
	private String strFileTableMap;
	private CompiledScript expFileTableMap;
	private boolean mergeTable = false;
	
	private GroupOp getGroupOp(String keyPrefix){
		String groupKey = keyPrefix==null? cfgkey_aggr_groupkey:keyPrefix+"."+cfgkey_aggr_groupkey;
		String groupKeyExp = keyPrefix==null? cfgkey_aggr_groupkey_exp:keyPrefix+"."+cfgkey_aggr_groupkey_exp;
		String groupKeyExpType = keyPrefix==null? cfgkey_aggr_groupkey_exp_type:keyPrefix+"."+cfgkey_aggr_groupkey_exp_type;
		String groupKeyExpName = keyPrefix==null? cfgkey_aggr_groupkey_exp_name:keyPrefix+"."+cfgkey_aggr_groupkey_exp_name;
		List<IdxRange> commonGroupKeys = IdxRange.parseString(pc.getString(groupKey));
		String[] groupKeyExps = pc.getStringArray(groupKeyExp);
		String[] groupKeyExpTypes = pc.getStringArray(groupKeyExpType);
		String[] groupKeyExpNames = pc.getStringArray(groupKeyExpName);
		CompiledScript[] groupKeyExpScripts = null;
		if (groupKeyExps!=null){
			groupKeyExpScripts = new CompiledScript[groupKeyExps.length];
			for (int i=0; i<groupKeyExpScripts.length; i++){
				String exp = groupKeyExps[i];
				groupKeyExpScripts[i] = ScriptEngineUtil.compileScript(exp);
			}
		}
		return new GroupOp(groupKeyExpNames, groupKeyExpTypes, groupKeyExps, groupKeyExpScripts, commonGroupKeys);
	}
	
	public CsvAggregateCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		if (pc.containsKey(cfgkey_aggr_old_table)){
			oldTables = pc.getStringArray(cfgkey_aggr_old_table);
		}
		if (pc.containsKey(cfgkey_aggr_new_table)){
			newTables = pc.getStringArray(cfgkey_aggr_new_table);
		}
		if (oldTables==null){//not using schema
			String[] strAggrOpList = pc.getStringArray(cfgkey_aggr_op);
			AggrOpMap aoMap = new AggrOpMap(strAggrOpList);
			aoMapMap.put(SINGLE_TABLE, aoMap);
			GroupOp groupOp = getGroupOp(null);
			groupKeysMap.put(SINGLE_TABLE, groupOp);
		}else{
			for (String tableName: oldTables){
				AggrOpMap aoMap = null;
				GroupOp groupOp = null;
				if (pc.containsKey(tableName+"."+cfgkey_aggr_op)){
					aoMap = new AggrOpMap(pc.getStringArray(tableName+"."+cfgkey_aggr_op));
					String groupKey = tableName+"."+cfgkey_aggr_groupkey;
					String groupKeyExp = tableName+"."+cfgkey_aggr_groupkey_exp;
					if (pc.containsKey(groupKey) || pc.containsKey(groupKeyExp)){
						groupOp = getGroupOp(tableName);
					}else{//for merge tables
						groupOp = getGroupOp(null);
					}
				}else if (pc.containsKey(cfgkey_aggr_op)){
					aoMap = new AggrOpMap(pc.getStringArray(cfgkey_aggr_op));
					groupOp = getGroupOp(null);
				}else{
					logger.error(String.format("aggr_op not configured for table %s", tableName));
				}
				aoMapMap.put(tableName, aoMap);
				groupKeysMap.put(tableName, groupOp);
			}
		}
		strFileTableMap = pc.getString(cfgkey_file_table_map, null);
		logger.info(String.format("fileTableMap:%s", strFileTableMap));
		if (strFileTableMap!=null){
			expFileTableMap = ScriptEngineUtil.compileScript(strFileTableMap);
			logger.info(String.format("fileTableMapExp:%s", expFileTableMap));
		}
		this.mergeTable = (oldTables!=null && newTables!=null && oldTables.length>1 && newTables.length==1);
		if (mergeTable){
			Arrays.sort(oldTables);
		}
	}
	
	private List<String> getCsvFields(CSVRecord r, GroupOp groupOp){
		List<String> keys = new ArrayList<String>();
		if (groupOp.getExpGroupExpScripts()!=null){
			String[] fields = new String[r.size()];
			for (int i=0; i<fields.length; i++){
				fields[i] = r.get(i);
			}
			super.getSystemVariables().put(ETLCmd.VAR_FIELDS, fields);
			for (CompiledScript cs:groupOp.getExpGroupExpScripts()){
				keys.add(ScriptEngineUtil.eval(cs, super.getSystemVariables()));
			}
		}
		
		List<IdxRange> irl = groupOp.getCommonGroupIdx();
		for (IdxRange ir: irl){
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd()==-1){
				end = r.size()-1;
			}
			for (int i=start; i<=end; i++){
				keys.add(r.get(i));
			}
		}
		return keys;
	}
	
	private List<String> updateSchema(Map<String, List<String>> attrsMap, Map<String, List<FieldType>> attrTypesMap){
		boolean schemaUpdated = false;
		List<String> createTableSqls = new ArrayList<String>();
		for (String newTable: attrsMap.keySet()){
			List<String> newAttrs = attrsMap.get(newTable);
			List<FieldType> newTypes = attrTypesMap.get(newTable);
			if (!logicSchema.hasTable(newTable)){
				//update schema
				logicSchema.updateTableAttrs(newTable, newAttrs);
				logicSchema.updateTableAttrTypes(newTable, newTypes);
				schemaUpdated = true;
				//generate create table
				createTableSqls.add(DBUtil.genCreateTableSql(newAttrs, newTypes, newTable, 
						dbPrefix, super.getDbtype()));
			}else{
				List<String> existNewAttrs = logicSchema.getAttrNames(newTable);
				List<FieldType> existNewAttrTypes = logicSchema.getAttrTypes(newTable);
				if (existNewAttrs.containsAll(newAttrs)){//
					//do nothing
				}else{
					logger.info(String.format("exist attrs for table %s:%s", newTable, existNewAttrs));
					logger.info(String.format("new attrs for table %s:%s", newTable, newAttrs));
					//update schema, happens only when the schema is updated by external force
					logicSchema.updateTableAttrs(newTable, newAttrs);
					logicSchema.updateTableAttrTypes(newTable, newTypes);
					schemaUpdated = true;
					//generate alter table
					newAttrs.removeAll(existNewAttrs);
					newTypes.removeAll(existNewAttrTypes);
					createTableSqls.addAll(DBUtil.genUpdateTableSql(newAttrs, newTypes, newTable, 
							dbPrefix, super.getDbtype()));
				}
			}
		}
		if (schemaUpdated){
			return super.updateDynSchema(createTableSqls);
		}else{
			return null;
		}
	}
	//single process for schema update
	@Override
	public List<String> sgProcess(){
		Map<String, List<String>> attrsMap = new HashMap<String, List<String>>();
		Map<String, List<FieldType>> attrTypesMap = new HashMap<String, List<FieldType>>();
		if (!mergeTable){//one to one aggr
			for (int tbIdx=0; tbIdx<oldTables.length; tbIdx++){
				String oldTable = oldTables[tbIdx];
				String newTable = newTables[tbIdx];
				List<String> attrs = logicSchema.getAttrNames(oldTable);
				List<FieldType> attrTypes = logicSchema.getAttrTypes(oldTable);
				int idxMax = attrs.size()-1;
				List<String> newAttrs = new ArrayList<String>();
				List<FieldType> newTypes = new ArrayList<FieldType>();
				List<IdxRange> commonGroupKeys = groupKeysMap.get(oldTable).getCommonGroupIdx();
				AggrOp aop = new AggrOp(AggrOperator.group, commonGroupKeys);
				AggrOpMap aoMap = aoMapMap.get(oldTable);
				aoMap.addAggrOp(aop);
				aoMap.constructMap(idxMax);
				for (int i=0; i<=idxMax; i++){
					if (aoMap.getOp(i)!=null){
						newAttrs.add(attrs.get(i));
						newTypes.add(attrTypes.get(i));
					}
				}
				String[] groupExpNames = groupKeysMap.get(oldTable).getExpGroupNames();
				String[] groupExpTypes = groupKeysMap.get(oldTable).getExpGroupTypes();
				if (groupExpNames!=null){
					for (int i=0; i<groupExpNames.length; i++){
						newAttrs.add(0, groupExpNames[groupExpNames.length-1-i]);
						newTypes.add(0, new FieldType(groupExpTypes[groupExpNames.length-1-i]));
					}
				}
				attrsMap.put(newTable, newAttrs);
				attrTypesMap.put(newTable, newTypes);
			}
		}else{//many to one aggr rows and merge tables
			String firstTable = oldTables[0];
			List<IdxRange> commonGroupKeys = groupKeysMap.get(firstTable).getCommonGroupIdx();
			AggrOp aop = new AggrOp(AggrOperator.group, commonGroupKeys);
			AggrOpMap groupAOMap = new AggrOpMap();
			groupAOMap.addAggrOp(aop);
			List<String> attrs = logicSchema.getAttrNames(firstTable);
			List<FieldType> attrTypes = logicSchema.getAttrTypes(firstTable);
			List<String> newAttrs = new ArrayList<String>();
			List<FieldType> newTypes = new ArrayList<FieldType>();
			int idxMax = attrs.size()-1;
			groupAOMap.constructMap(idxMax);
			for (int i=0; i<=idxMax; i++){
				if (groupAOMap.getOp(i)!=null){
					newAttrs.add(attrs.get(i)); //use common attr name
					newTypes.add(attrTypes.get(i));
				}
			}
			String[] groupExpNames = groupKeysMap.get(firstTable).getExpGroupNames();
			String[] groupExpTypes = groupKeysMap.get(firstTable).getExpGroupTypes();
			if (groupExpNames!=null){
				for (int i=0; i<groupExpNames.length; i++){
					newAttrs.add(0, groupExpNames[groupExpNames.length-1-i]);//use common attr name
					newTypes.add(0, new FieldType(groupExpTypes[groupExpNames.length-1-i]));
				}
			}
			for (String oldTable: oldTables){
				attrs = logicSchema.getAttrNames(oldTable);
				attrTypes = logicSchema.getAttrTypes(oldTable);
				AggrOpMap aoMap = aoMapMap.get(oldTable);
				idxMax = logicSchema.getAttrNames(oldTable).size()-1;
				aoMap.constructMap(idxMax);
				for (int i=0; i<=idxMax; i++){
					if (aoMap.getOp(i)!=null){
						newAttrs.add(oldTable+"_"+attrs.get(i));//use updated name
						newTypes.add(attrTypes.get(i));
					}
				}
			}
			attrsMap.put(newTables[0], newAttrs);
			attrTypesMap.put(newTables[0], newTypes);
		}
		return updateSchema(attrsMap, attrTypesMap);
	}

	@Override
	public Map<String, String> reduceMapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		try {
			String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			Map<String, Object> varMap = new HashMap<String, Object>();
			varMap.put(FileETLCmd.VAR_NAME_FILE_NAME, inputFileName);
			CSVParser parser = CSVParser.parse(row, CSVFormat.DEFAULT);
			List<CSVRecord> csvl = parser.getRecords();
			String tableKey = SINGLE_TABLE;
			for (CSVRecord r: csvl){
				GroupOp groupKeys = null;
				if (oldTables==null){
					groupKeys = groupKeysMap.get(SINGLE_TABLE);
				}else{
					String fileKey = inputFileName;
					if (expFileTableMap!=null){
						fileKey = ScriptEngineUtil.eval(expFileTableMap, varMap);
					}
					logger.info(String.format("tableName:%s from inputFileName:%s", fileKey, inputFileName));
					if (groupKeysMap.containsKey(fileKey)){
						tableKey = fileKey;
						groupKeys = groupKeysMap.get(fileKey);
					}else{
						logger.info(String.format("groupKeysMap %s does not have inputFileName %s", groupKeysMap.keySet(), fileKey));
						break;
					}
				}
				List<String> keys = getCsvFields(r, groupKeys);
				String v = row;
				if (!mergeTable){
					keys.add(0, tableKey);
				}else{
					v = tableKey + "," + row;
				}
				String newKey = Util.getCsv(keys, false);
				logger.debug(String.format("key:%s, value:%s", newKey, v));
				context.write(new Text(newKey), new Text(v));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	/**
	 * @param tableName
	 * @param rl, is null, return the record filled with empty/0 values for outer join
	 * @return
	 */
	private List<String> getAggrValues(String tableName, List<CSVRecord> rl){
		int idxMax=0;
		if (rl==null){
			idxMax = logicSchema.getAttrNames(tableName).size()-1;
		}else{
			idxMax = rl.get(0).size()-1;
		}
		AggrOpMap aoMap = aoMapMap.get(tableName);
		aoMap.constructMap(idxMax);
		List<String> aggrValues = new ArrayList<String>();
		for (int i=0; i<=idxMax; i++){
			AggrOperator op = aoMap.getOp(i);
			if (op!=null){
				if (rl!=null){
					if (AggrOperator.sum==op){
						float av=0;
						for (CSVRecord r:rl){
							String strv = r.get(i);
							float v =0f;
							if (!"".equals(strv.trim())){
								v= Float.parseFloat(strv);		
							}
							av +=v;
						}
						aggrValues.add(Float.toString(av));
					}else{
						logger.error(String.format("op %s not supported yet.", op.toString()));
					}
				}else{
					aggrValues.add(Float.toString(0));
				}
			}
		}
		return aggrValues;
	}
	
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values){
		List<String[]> rets = new ArrayList<String[]>();
		if (!mergeTable){
			String[] kl = key.toString().split(KEY_SEP);
			String tableName = kl[0];
			List<String> ks = new ArrayList<String>();
			for (int i=1; i<kl.length; i++){
				ks.add(kl[i]);
			}
			String newKey = String.join(KEY_SEP, ks);
			List<CSVRecord> rl = new ArrayList<CSVRecord>();
			for (Text v: values){
				try {
					CSVParser parser = CSVParser.parse(v.toString(), CSVFormat.DEFAULT);
					rl.addAll(parser.getRecords());
				}catch(Exception e){
					logger.error("", e);
				}
			}
			String[] ret = new String[]{newKey, Util.getCsv(getAggrValues(tableName, rl), false), tableName};
			rets.add(ret);
		}else{
			TreeMap<String, List<CSVRecord>> sortedTableValuesMap = new TreeMap<String, List<CSVRecord>>();
			for (Text v: values){
				try {
					String s = v.toString();
					int firstComma =s.indexOf(",");
					String tableName = s.substring(0, firstComma);
					String vs = s.substring(firstComma+1);
					CSVParser parser = CSVParser.parse(vs, CSVFormat.DEFAULT);
					List<CSVRecord> rl = new ArrayList<CSVRecord>();
					rl.addAll(parser.getRecords());
					List<CSVRecord> csvrl = sortedTableValuesMap.get(tableName);
					if (csvrl==null) {
						csvrl = new ArrayList<CSVRecord>();
						sortedTableValuesMap.put(tableName, csvrl);
					}
					csvrl.addAll(rl);
				}catch(Exception e){
					logger.error("", e);
				}
			}
			List<String> aggrValues = new ArrayList<String>();
			for (int i=0; i<oldTables.length; i++){
				String tableName = oldTables[i];
				List<CSVRecord> csvrl = sortedTableValuesMap.get(tableName);
				List<String> avs = getAggrValues(tableName, csvrl);
				aggrValues.addAll(avs);
				
			}
			String[] ret = new String[]{key.toString(), Util.getCsv(aggrValues, false), newTables[0]};
			rets.add(ret);
		}
		return rets;
	}
}