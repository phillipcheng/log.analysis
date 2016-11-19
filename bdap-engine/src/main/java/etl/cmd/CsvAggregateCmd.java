package etl.cmd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.script.CompiledScript;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import bdap.util.Util;

import org.apache.spark.api.java.function.Function;

import etl.cmd.transform.AggrOp;
import etl.cmd.transform.AggrOps;
import etl.cmd.transform.GroupOp;
import etl.engine.AggrOperator;
import etl.engine.ETLCmd;
import etl.util.FieldType;
import etl.util.IdxRange;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;
import scala.Tuple2;
import scala.Tuple3;

public class CsvAggregateCmd extends SchemaETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LogManager.getLogger(CsvAggregateCmd.class);

	public static final String AGGR_OP_SUM="sum";
	public static final String AGGR_OPERATOR_SEP="\\|";
	
	//cfgkey
	public static final String cfgkey_input_endwithcomma="input.endwithcomma";
	public static final String cfgkey_aggr_op="aggr.op";
	public static final String cfgkey_aggr_groupkey="aggr.groupkey";
	public static final String cfgkey_aggr_groupkey_exp="aggr.groupkey.exp";
	public static final String cfgkey_aggr_groupkey_exp_type="aggr.groupkey.exp.type";
	public static final String cfgkey_aggr_groupkey_exp_name="aggr.groupkey.exp.name";
	public static final String cfgkey_aggr_old_table="old.table";
	public static final String cfgkey_aggr_new_table="new.table";
	
	private boolean inputEndWithComma=false;
	private int oldTableCnt=0;
	private String[][] oldTables = null;
	private String[] newTables = null;
	private transient Map<String, List<String>> oldnewTableMap;
	private transient Map<String, AggrOps> aoMapMap; //old table name to AggrOps
	private transient Map<String, GroupOp> groupKeysMap; //old table name to groupOp
	
	private boolean mergeTable = false;
	
	private GroupOp getGroupOp(String keyPrefix){
		String groupKey = keyPrefix==null? cfgkey_aggr_groupkey:keyPrefix+"."+cfgkey_aggr_groupkey;
		String groupKeyExp = keyPrefix==null? cfgkey_aggr_groupkey_exp:keyPrefix+"."+cfgkey_aggr_groupkey_exp;
		String groupKeyExpType = keyPrefix==null? cfgkey_aggr_groupkey_exp_type:keyPrefix+"."+cfgkey_aggr_groupkey_exp_type;
		String groupKeyExpName = keyPrefix==null? cfgkey_aggr_groupkey_exp_name:keyPrefix+"."+cfgkey_aggr_groupkey_exp_name;
		List<IdxRange> commonGroupKeys = IdxRange.parseString(super.getCfgString(groupKey, null));
		String[] groupKeyExps = super.getCfgStringArray(groupKeyExp);
		String[] groupKeyExpTypes = super.getCfgStringArray(groupKeyExpType);
		String[] groupKeyExpNames = super.getCfgStringArray(groupKeyExpName);
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
	
	public CsvAggregateCmd(){
		super();
	}
	
	public CsvAggregateCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public CsvAggregateCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		inputEndWithComma = super.getCfgBoolean(cfgkey_input_endwithcomma, false);
		aoMapMap = new HashMap<String, AggrOps>();
		groupKeysMap = new HashMap<String, GroupOp>();
		if (super.cfgContainsKey(cfgkey_aggr_old_table)){
			String[] oldTableGroups = super.getCfgStringArray(cfgkey_aggr_old_table);
			oldTables = new String[oldTableGroups.length][];
			for (int i=0; i<oldTables.length;i++){
				oldTables[i]=oldTableGroups[i].split(";");
				oldTableCnt+=oldTables[i].length;
			}
		}
		if (super.cfgContainsKey(cfgkey_aggr_new_table)){
			newTables = super.getCfgStringArray(cfgkey_aggr_new_table);
		}
		if (oldTables==null){//not using schema
			String[] strAggrOpList = super.getCfgStringArray(cfgkey_aggr_op);
			AggrOps aoMap = new AggrOps(strAggrOpList);
			aoMapMap.put(SINGLE_TABLE, aoMap);
			GroupOp groupOp = getGroupOp(null);
			groupKeysMap.put(SINGLE_TABLE, groupOp);
		}else{
			oldnewTableMap = new HashMap<String, List<String>>();
			for (int i=0; i<oldTables.length; i++){
				for (int j=0; j<oldTables[i].length; j++){
					String tableName = oldTables[i][j];
					String newTable = newTables[i];
					List<String> newTables = oldnewTableMap.get(tableName);
					if (newTables==null){
						newTables = new ArrayList<String>();
						oldnewTableMap.put(tableName, newTables);
					}
					newTables.add(newTable);
					AggrOps aoMap = null;
					GroupOp groupOp = null;
					if (super.cfgContainsKey(tableName+"."+cfgkey_aggr_op)){
						aoMap = new AggrOps(super.getCfgStringArray(tableName+"."+cfgkey_aggr_op));
						String groupKey = tableName+"."+cfgkey_aggr_groupkey;
						String groupKeyExp = tableName+"."+cfgkey_aggr_groupkey_exp;
						if (super.cfgContainsKey(groupKey) || super.cfgContainsKey(groupKeyExp)){
							groupOp = getGroupOp(tableName);
						}else{//for merge tables
							groupOp = getGroupOp(null);
						}
					}else if (super.cfgContainsKey(cfgkey_aggr_op)){
						aoMap = new AggrOps(super.getCfgStringArray(cfgkey_aggr_op));
						groupOp = getGroupOp(null);
					}else{
						logger.error(String.format("aggr_op not configured for table %s", tableName));
					}
					aoMapMap.put(tableName, aoMap);
					groupKeysMap.put(tableName, groupOp);
				}
			}
		}
		this.mergeTable = oldnewTableMap!=null && this.oldTableCnt>this.newTables.length;
		if (oldTables!=null){
			for (String[] oldTableGrp:oldTables){//sort table by name order
				Arrays.sort(oldTableGrp);
			}
		}
	}
	
	//list of keys
	private List<List<String>> getCsvFields(CSVRecord r, GroupOp groupOp){
		List<List<String>> keys = new ArrayList<List<String>>();
		if (groupOp.getExpGroupExpScripts()!=null && groupOp.getExpGroupExpScripts().length>0){
			String[] fields = new String[r.size()];
			for (int i=0; i<fields.length; i++){
				fields[i] = r.get(i);
			}
			super.getSystemVariables().put(ETLCmd.VAR_FIELDS, fields);
			for (CompiledScript cs:groupOp.getExpGroupExpScripts()){
				Object ret = ScriptEngineUtil.evalObject(cs, super.getSystemVariables());
				List<String> slist = null;
				boolean resArray=false;
				if (ret instanceof String[]){
					slist = Arrays.asList((String[])ret);
					resArray=true;
				}else if (ret instanceof String){
					slist = Arrays.asList(new String[]{(String)ret});
				}
				if (keys.size()==0){//first time
					for (String ks:slist){
						List<String> cks = new ArrayList<String>();
						cks.addAll(Arrays.asList(ks.split(",")));
						keys.add(cks);
					}
				}else{//add to corresponding list
					for (int i=0;i<keys.size(); i++){
						List<String> ll = keys.get(i);
						if (resArray){
							ll.addAll(Arrays.asList(slist.get(i).split(",")));
						}else{
							ll.addAll(slist);
						}
					}
				}
			}
		}else{//one empty key entry should be created
			keys.add(new ArrayList<String>());
		}
		
		List<IdxRange> irl = groupOp.getCommonGroupIdx();
		for (IdxRange ir: irl){
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd()==-1){
				end = r.size()-1;
			}
			for (int i=start; i<=end; i++){
				for (List<String> sk:keys){
					sk.add(r.get(i));
				}
			}
		}
		return keys;
	}
	
	private void addCountField(List<String> attrs, List<FieldType> attrTypes){
		attrs.add("cnt");
		attrTypes.add(new FieldType(VarType.INT));
	}
	public <T> List<T> getIdxedList(List<T> input, List<IdxRange> irl, int idxMax){
		List<T> tlist = new ArrayList<T>();
		List<Integer> idxList = GroupOp.getIdxInRange(irl, idxMax);
		for (int i:idxList){
			tlist.add(input.get(i));
		}
		return tlist;
	}
	
	Map<String, List<String>> attrsMap = new HashMap<String, List<String>>();
	Map<String, List<FieldType>> attrTypesMap = new HashMap<String, List<FieldType>>();
	//single process for schema update
	@Override
	public List<String> sgProcess(){
		if (!mergeTable){//one to one aggr
			for (int tbIdx=0; tbIdx<oldTables.length; tbIdx++){
				String oldTable = oldTables[tbIdx][0];
				String newTable = newTables[tbIdx];
				List<String> attrs = logicSchema.getAttrNames(oldTable);
				List<FieldType> attrTypes = logicSchema.getAttrTypes(oldTable);
				int idxMax = attrs.size();
				List<IdxRange> commonGroupKeys = groupKeysMap.get(oldTable).getCommonGroupIdx();
				
				List<String> newAttrs = new ArrayList<String>();
				List<FieldType> newTypes = new ArrayList<FieldType>();
				//add group by common fields
				newAttrs.addAll(getIdxedList(attrs, commonGroupKeys, idxMax));
				newTypes.addAll(getIdxedList(attrTypes, commonGroupKeys, idxMax));
				//prepend exp(calculated fields)
				String[] groupExpNames = groupKeysMap.get(oldTable).getExpGroupNames();
				String[] groupExpTypes = groupKeysMap.get(oldTable).getExpGroupTypes();
				if (groupExpNames!=null){
					for (int i=0; i<groupExpNames.length; i++){
						newAttrs.add(0, groupExpNames[groupExpNames.length-1-i]);
						newTypes.add(0, new FieldType(groupExpTypes[groupExpNames.length-1-i]));
					}
				}
				//add aggregate fields op by op
				List<AggrOp> aol = this.aoMapMap.get(oldTable).getOpList();
				for (AggrOp aop:aol){
					if (aop.getAo()!=AggrOperator.count){
						newAttrs.addAll(getIdxedList(attrs, aop.getIdxRangeList(), idxMax));
						newTypes.addAll(getIdxedList(attrTypes, aop.getIdxRangeList(), idxMax));
					}else{
						addCountField(newAttrs, newTypes);
					}
				}
				attrsMap.put(newTable, newAttrs);
				attrTypesMap.put(newTable, newTypes);
			}
		}else{//many to one aggr rows and merge tables
			for (int idx=0; idx<oldTables.length; idx++){
				String[] oldTableGroup=oldTables[idx];
				String newTableName = newTables[idx];
				String firstTable = oldTableGroup[0];
				List<IdxRange> commonGroupKeys = groupKeysMap.get(firstTable).getCommonGroupIdx();
				List<String> attrs = logicSchema.getAttrNames(firstTable);
				List<FieldType> attrTypes = logicSchema.getAttrTypes(firstTable);
				List<String> newAttrs = new ArrayList<String>();
				List<FieldType> newTypes = new ArrayList<FieldType>();
				int idxMax=attrs.size();
				//add group by common fields
				newAttrs.addAll(getIdxedList(attrs, commonGroupKeys, idxMax));
				newTypes.addAll(getIdxedList(attrTypes, commonGroupKeys, idxMax));
				//prepend exp(calculated fields)
				String[] groupExpNames = groupKeysMap.get(firstTable).getExpGroupNames();
				String[] groupExpTypes = groupKeysMap.get(firstTable).getExpGroupTypes();
				if (groupExpNames!=null){
					for (int i=0; i<groupExpNames.length; i++){
						newAttrs.add(0, groupExpNames[groupExpNames.length-1-i]);//use common attr name
						newTypes.add(0, new FieldType(groupExpTypes[groupExpNames.length-1-i]));
					}
				}
				//add aggregate fields table by table, op by op
				for (String oldTable: oldTableGroup){
					attrs = logicSchema.getAttrNames(oldTable);
					attrTypes = logicSchema.getAttrTypes(oldTable);
					int attrNum = logicSchema.getAttrNames(oldTable).size();
					List<AggrOp> aol = aoMapMap.get(oldTable).getOpList();
					for (AggrOp aop:aol){
						if (aop.getAo()!=AggrOperator.count){
							for (String attrName:getIdxedList(attrs, aop.getIdxRangeList(), attrNum)){
								newAttrs.add(oldTable+"_"+attrName);
							}
							newTypes.addAll(getIdxedList(attrTypes, aop.getIdxRangeList(), attrNum));
						}else{
							addCountField(newAttrs, newTypes);
						}
					}
				}
				attrsMap.put(newTableName, newAttrs);
				attrTypesMap.put(newTableName, newTypes);
			}
		}
		return updateSchema(attrsMap, attrTypesMap);
	}
	
	public void genSchemaSql(String lsFile, String sqlFile){
		sgProcess();
		super.genSchemaSql(attrsMap, attrTypesMap, lsFile, sqlFile);
	}

	//tableKey.aggrKeys,aggrValues
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value){
		super.init();
		List<Tuple2<String,String>> ret = new ArrayList<Tuple2<String,String>>();
		try {
			String row = value.toString();
			CSVParser parser = CSVParser.parse(row, CSVFormat.DEFAULT);
			List<CSVRecord> csvl = parser.getRecords();
			String oldTableName = SINGLE_TABLE;
			for (CSVRecord r: csvl){
				GroupOp groupKeys = null;
				if (oldTables==null){
					groupKeys = groupKeysMap.get(SINGLE_TABLE);
				}else{
					if (groupKeysMap.containsKey(tableName)){
						oldTableName = tableName;
						groupKeys = groupKeysMap.get(tableName);
					}else{
						logger.debug(String.format("groupKeysMap %s does not have table %s", groupKeysMap.keySet(), tableName));
						break;
					}
				}
				String v = row;
				if (!mergeTable){
					List<List<String>> keys = getCsvFields(r, groupKeys);
					for (List<String> oKey:keys){
						oKey.add(0, oldTableName);
						String newKey = Util.getCsv(oKey, false);
						logger.debug(String.format("key:%s, value:%s", newKey, v));
						ret.add(new Tuple2<String, String>(newKey, v));
					}
				}else{
					List<String> newTableNames = oldnewTableMap.get(tableName);
					v = oldTableName + KEY_SEP + row;
					for (String newTableName:newTableNames){
						List<List<String>> keys = getCsvFields(r, groupKeys);
						for (List<String> okey:keys){
							okey.add(0, newTableName);
							String newKey = Util.getCsv(okey, false);
							logger.debug(String.format("key:%s, value:%s", newKey, v));
							ret.add(new Tuple2<String, String>(newKey, v));
						}
					}
				}
				
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return ret;
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		//process skip header
		if (skipHeader && offset==0) {
			logger.info("skip header:" + row);
			return null;
		}
		if (inputEndWithComma){
			row = row.trim();
			if (row.endsWith(",")){
				row = row.replaceAll(",$", "");
			}
		}
		try {
			String tableName = getTableName(context);
			logger.debug(String.format("in map tableName:%s", tableName));
			if (tableName==null || "".equals(tableName.trim())){
				logger.error(String.format("tableName got is empty from exp %s and fileName %s", super.getStrFileTableMap(), 
						((FileSplit) context.getInputSplit()).getPath().getName()));
			}else{
				List<Tuple2<String, String>> it = flatMapToPair(tableName, row);
				for (Tuple2<String,String> t : it){
					context.write(new Text(t._1), new Text(t._2));
				}
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
		AggrOps aoMap = aoMapMap.get(tableName);
		List<String> aggrValues = new ArrayList<String>();
		int fieldNum=0;
		if (rl!=null && rl.size()>0){
			fieldNum=rl.get(0).size();
		}else{
			if (logicSchema!=null){
				if (logicSchema.getAttrNameMap().containsKey(tableName)){
					fieldNum=logicSchema.getAttrNameMap().get(tableName).size();
				}else{
					logger.error(String.format("table %s not found in logic schema defined.", tableName));
					return null;
				}
			}else{
				logger.error(String.format("no logic schema defined."));
				return null;
			}
		}
		for (AggrOp aop: aoMap.getOpList()){
			AggrOperator op = aop.getAo();
			if (AggrOperator.count==op){
				aggrValues.add(Integer.toString(rl.size()));
			}else{
				List<Integer> idxList = GroupOp.getIdxInRange(aop.getIdxRangeList(), fieldNum);
				for (int idx:idxList){
					float aggrValue=0;
					if (rl!=null && rl.size()>0){
						for (CSVRecord r:rl){
							String strv = r.get(idx);
							float v =0f;
							if (!"".equals(strv.trim())){
								v= Float.parseFloat(strv);
							}
							if (AggrOperator.sum==op || AggrOperator.avg==op){
								aggrValue +=v;
							}else if (AggrOperator.max==op){
								if (v>aggrValue) aggrValue=v;
							}else if (AggrOperator.min==op){
								if (v<aggrValue) aggrValue=v;
							}else{
								logger.error(String.format("op %s not supported yet.", op.toString()));
							}
						}
						if (AggrOperator.avg==op){
							aggrValue=aggrValue/rl.size();
						}
					}
					aggrValues.add(Float.toString(aggrValue));
				}
			}
		}
		return aggrValues;
	}
	
	public Tuple3<String, String, String> reduceByKey(String key, Iterable<String> it){
		super.init();
		String[] kl = key.toString().split(KEY_SEP, -1);
		String tableName = kl[0];
		List<String> ks = new ArrayList<String>();
		for (int i=1; i<kl.length; i++){
			ks.add(kl[i]);
		}
		String newKey = String.join(KEY_SEP, ks);
		if (!mergeTable){
			List<CSVRecord> rl = new ArrayList<CSVRecord>();
			Iterator<String> its = it.iterator();
			while (its.hasNext()){
				String v = its.next();
				logger.debug(String.format("reduce: key:%s, one value:%s", key, v));
				try {
					CSVParser parser = CSVParser.parse(v.toString(), CSVFormat.DEFAULT);
					rl.addAll(parser.getRecords());
				}catch(Exception e){
					logger.error("", e);
				}
			}
			String newTableName = tableName;
			if (oldnewTableMap!=null && oldnewTableMap.containsKey(tableName)){
				newTableName = oldnewTableMap.get(tableName).get(0);
			}
			return new Tuple3<String, String, String>(newKey, 
					Util.getCsv(getAggrValues(tableName, rl), false), 
					newTableName);
		}else{
			int grpIdx = Arrays.asList(newTables).indexOf(tableName);
			if (grpIdx==-1){
				logger.error(String.format("%s not found in new tables %s", tableName, Arrays.asList(newTables)));
			}
			Map<String, List<CSVRecord>> oldTableRecordsMap = new HashMap<String, List<CSVRecord>>();
			Iterator<String> its = it.iterator();
			while (its.hasNext()){
				try {
					String s = its.next().toString();
					logger.debug(String.format("reduce: key:%s, one value:%s", key, s));
					int firstComma =s.indexOf(KEY_SEP);
					String oldTableName = s.substring(0, firstComma);
					String vs = s.substring(firstComma+1);
					CSVParser parser = CSVParser.parse(vs, CSVFormat.DEFAULT);
					List<CSVRecord> rl = new ArrayList<CSVRecord>();
					rl.addAll(parser.getRecords());
					List<CSVRecord> csvrl = oldTableRecordsMap.get(oldTableName);
					if (csvrl==null) {
						csvrl = new ArrayList<CSVRecord>();
						oldTableRecordsMap.put(oldTableName, csvrl);
					}
					csvrl.addAll(rl);
				}catch(Exception e){
					logger.error("", e);
				}
			}
			List<String> aggrValues = new ArrayList<String>();
			for (int i=0; i<oldTables[grpIdx].length; i++){
				String oldTableName = oldTables[grpIdx][i];
				List<CSVRecord> csvrl = oldTableRecordsMap.get(oldTableName);
				List<String> avs = getAggrValues(oldTableName, csvrl);
				aggrValues.addAll(avs);
			}
			return new Tuple3<String, String, String>(
					newKey.toString(), 
					Util.getCsv(aggrValues, false), 
					newTables[grpIdx]);
		}
	}
	
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos){
		List<String> svalues = new ArrayList<String>();
		Iterator<Text> vit = values.iterator();
		while (vit.hasNext()){
			svalues.add(vit.next().toString());
		}
		Tuple3<String, String, String> ret = reduceByKey(key.toString(), svalues);
		List<String[]> retlist = new ArrayList<String[]>();
		retlist.add(new String[]{ret._1(), ret._2(), ret._3()});
		return retlist;
	}
	
	@Override
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc){
		JavaPairRDD<String, String> csvgroup = input.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
				List<Tuple2<String, String>> ret = flatMapToPair(t._1, t._2);
				return ret.iterator();
			}
		});
		
		JavaPairRDD<String, String> csvaggr = csvgroup.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
				Tuple3<String, String, String> t3 = reduceByKey(t._1, t._2);
				return new Tuple2<String, String>(t3._3(), t3._1() + KEY_SEP + t3._2());
			}
		});
		
		return csvaggr;
		
	}
}