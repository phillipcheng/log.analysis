package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.script.CompiledScript;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import etl.cmd.transform.AggrOp;
import etl.cmd.transform.AggrOps;
import etl.cmd.transform.GroupOp;
import etl.engine.AggrOperator;
import etl.engine.ETLCmd;
import etl.engine.JoinType;
import etl.engine.MRMode;
import etl.engine.ProcessMode;
import etl.util.FieldType;
import etl.util.IdxRange;
import etl.util.ScriptEngineUtil;
import etl.util.StringUtil;
import etl.util.VarType;
import scala.Tuple2;
import scala.Tuple3;

public class CsvMergeCmd extends SchemaETLCmd{
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = LogManager.getLogger(CsvMergeCmd.class);
	
	public static final String KEY_HEADER_SEP="----";
	public static final String MULTIPLE_KEY_SEP="##";
	
	
	//cfgkey
	public static final String cfgkey_src_entity="src.entity";
	public static final String cfgkey_dest_entity="dest.entity";
	public static final String cfgkey_src_keys="src.keys";
	public static final String cfgkey_src_skipHeader="src.skipHeader";
	public static final String cfgkey_join_type="join.type";
	public static final String cfgkey_ret_value="ret.value";
	//system varaibles
	public static final String KEY_SYSTEM_VAR_TABLE="table";
	public static final String KEY_SYSTEM_VAR_CSV="csv";
	
	//
	private Pattern[] srcFilesExp;
	private String[] srcTables;
	private String destTable;
	private List<IdxRange>[] srcKeys;
	private boolean[] srcSkipHeader;
	private JoinType joinType;
	private transient CompiledScript retValueExp;
	private int srcNum;
	
	public CsvMergeCmd(){
		super();
	}
	
	public CsvMergeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public CsvMergeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public CsvMergeCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		this.setMrMode(MRMode.line);
		String[] srcFiles =super.getCfgStringArray(cfgkey_src_entity);
		srcNum = srcFiles.length;
		if (super.getLogicSchema()==null){
			srcFilesExp = new Pattern[srcNum];
			for (int i=0; i<srcNum; i++){
				String scriptExp = srcFiles[i];
				String globExp = (String) ScriptEngineUtil.eval(scriptExp, VarType.STRING, super.getSystemVariables());
				srcFilesExp[i] = Pattern.compile(StringUtil.convertGlobToRegEx(globExp));
			}
		}else{
			srcTables = srcFiles;
			destTable = super.getCfgString(cfgkey_dest_entity, null);
		}
		String[] srcKeysArr = super.getCfgStringArray(cfgkey_src_keys);
		srcKeys = new List[srcNum];
		for (int i=0; i<srcNum; i++){
			srcKeys[i] = IdxRange.parseString(srcKeysArr[i]);
		}
		String[] srcSkipHeaderArr = super.getCfgStringArray(cfgkey_src_skipHeader);
		srcSkipHeader = new boolean[srcNum];
		for (int i=0; i<srcNum; i++){
			srcSkipHeader[i] = Boolean.parseBoolean(srcSkipHeaderArr[i]);
		}
		joinType = JoinType.valueOf(super.getCfgString(cfgkey_join_type, JoinType.inner.toString()));
		String retValueStr = super.getCfgString(cfgkey_ret_value, null);
		if (retValueStr!=null && !"".equals(retValueStr.trim())){
			retValueExp = ScriptEngineUtil.compileScript(retValueStr);
		}
	}
	
	
	//single process for schema update
	@Override
	public List<String> sgProcess(){
		if (this.logicSchema==null)
			return null;
		Map<String, List<String>> attrsMap = new HashMap<String, List<String>>();
		Map<String, List<FieldType>> attrTypesMap = new HashMap<String, List<FieldType>>();
		
		List<String> newAttrs = new ArrayList<String>();
		List<FieldType> newTypes = new ArrayList<FieldType>();
		{
			String firstTable = srcTables[0];
			List<IdxRange> commonGroupKeys = this.srcKeys[0];
			List<String> attrs = logicSchema.getAttrNames(firstTable);
			List<FieldType> attrTypes = logicSchema.getAttrTypes(firstTable);
			List<Integer> groupKeyIdxList = GroupOp.getIdxInRange(commonGroupKeys, attrs.size());
			for (int i:groupKeyIdxList){
				newAttrs.add(attrs.get(i)); //use common attr name
				newTypes.add(attrTypes.get(i));
			}
		}
		for (int i=0; i<srcNum; i++){
			String oldTable = srcTables[i];
			List<IdxRange> commonGroupKeys = this.srcKeys[i];
			List<String> attrs = logicSchema.getAttrNames(oldTable);
			List<FieldType> attrTypes = logicSchema.getAttrTypes(oldTable);
			int attrNum = logicSchema.getAttrNames(oldTable).size();
			List<Integer> groupKeyIdxList = GroupOp.getIdxOutRange(commonGroupKeys, attrNum);
			for (int j:groupKeyIdxList){
				newAttrs.add(oldTable+"_"+attrs.get(j));
				newTypes.add(attrTypes.get(j));
			}
		}
		attrsMap.put(destTable, newAttrs);
		attrTypesMap.put(destTable, newTypes);
		
		return updateSchema(attrsMap, attrTypesMap);
	}
	
	private Tuple3<Integer, String, String> kv2kv(String tfName, String row){
		int idx=-1;
		if (super.getLogicSchema()!=null){
			for (int i=0; i<srcTables.length; i++){
				if (srcTables[i].equals(tfName)){
					idx=i;
					break;
				}
			}
			if (idx==-1){
				logger.error(String.format("%s matches nothing %s", tfName, Arrays.asList(srcTables)));
				return null;
			}
		}else{
			for (int i=0; i<srcFilesExp.length; i++){
				if (srcFilesExp[i].matcher(tfName).matches()){
					idx=i;
					break;
				}
			}
			if (idx==-1){
				logger.error(String.format("%s matches nothing %s", tfName, Arrays.asList(srcFilesExp)));
				return null;
			}
		}
		List<IdxRange> keyIdx = srcKeys[idx];
		try {
			CSVParser parser = CSVParser.parse(row, CSVFormat.DEFAULT);
			CSVRecord csv = parser.getRecords().get(0);
			List<String> keys = GroupOp.getFieldsInRange(csv, keyIdx);
			String value = String.format("%d%s%s", idx, KEY_HEADER_SEP, row);
			return new Tuple3<Integer, String, String>(idx, String.join(MULTIPLE_KEY_SEP, keys), value);
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	/**
	 * row: csv
	 */
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		Map<String, Object> retMap = new HashMap<String, Object>();
		String tfName=null;
		if (super.getLogicSchema()!=null){
			tfName = super.getTableName(context);
		}else{
			tfName = Path.getPathWithoutSchemeAndAuthority(((FileSplit) context.getInputSplit()).getPath()).toString();
		}
		Tuple3<Integer, String, String> ikv = kv2kv(tfName, row);
		int idx = ikv._1();
		if (srcSkipHeader[idx] && offset==0){
			return retMap;
		}
		List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
		vl.add(new Tuple2<String,String>(ikv._2(), ikv._3()));
		retMap.put(RESULT_KEY_OUTPUT_TUPLE2, vl);
		return retMap;
	}
	
	private List<String> getNonKeyValues(int srcIdx, CSVRecord csvr){
		int fieldNum=0;
		if (csvr==null){
			if (logicSchema!=null){
				String tableName = this.srcTables[srcIdx];
				fieldNum = logicSchema.getAttrNames(tableName).size();
			}else{
				logger.info(String.format("file merge with No.%d file-entry empty. ok if used with inner join", srcIdx));
			}
		}else{
			fieldNum = csvr.size();
		}
		return GroupOp.getFieldsOutRange(csvr, this.srcKeys[srcIdx], fieldNum);
	}
	
	private List<Tuple2<String, String>> kvs2kvlist(String key, Iterable<String> values){
		List<Tuple2<String, String>> retlist = new ArrayList<Tuple2<String, String>>();
		Iterator<String> it = values.iterator();
		CSVRecord[] mergedRecord = new CSVRecord[srcNum];
		int hitNum=0;
		while (it.hasNext()){
			try {
				String v = it.next().toString();
				logger.debug(String.format("key:%s, value:%s", key, v));
				int keyIdx = Integer.parseInt(v.substring(0, v.indexOf(KEY_HEADER_SEP)));
				String vs = v.substring(v.indexOf(KEY_HEADER_SEP)+KEY_HEADER_SEP.length());
				CSVRecord csvr = mergedRecord[keyIdx];
				if (csvr==null) {
					CSVParser parser = CSVParser.parse(vs, CSVFormat.DEFAULT);
					mergedRecord[keyIdx] = parser.getRecords().get(0);
					hitNum++;
					if (retValueExp!=null){
						this.getSystemVariables().put(KEY_SYSTEM_VAR_TABLE+keyIdx, vs.split(",", -1));
						this.getSystemVariables().put(KEY_SYSTEM_VAR_CSV+keyIdx, vs);
					}
				}else{
					logger.error(String.format("data for keyIdx %d exists %s: now comes another %s", keyIdx, csvr, vs));
				}
			}catch(Exception e){
				logger.error("", e);
			}
		}
		String ret = null;
		if (retValueExp!=null){
			ret = ScriptEngineUtil.eval(retValueExp, this.getSystemVariables());
		}else{//default return is merge all, put the joinKey at the beginning and then table contents from 1 to srcNum
			String keys = key.toString();
			List<String> dValues = new ArrayList<String>();
			dValues.addAll(Arrays.asList(keys.split(MULTIPLE_KEY_SEP)));
			for (int i=0; i<srcNum; i++){
				dValues.addAll(getNonKeyValues(i, mergedRecord[i]));
			}
			ret = String.join(",", dValues);
		}
			
		if (JoinType.inner==joinType){
			if (hitNum==srcNum){
				retlist.add(new Tuple2<String, String>(ETLCmd.SINGLE_TABLE, ret));
			}else{
				logger.warn(String.format("inner join, some data is missing, data from tables are %s", mergedRecord.toString()));
			}
		}else if (JoinType.outer == joinType){
			retlist.add(new Tuple2<String, String>(ETLCmd.SINGLE_TABLE,ret));
		}else{
			logger.error(String.format("join type:%s not supported.", joinType));
		}
		return retlist;
	}

	private Iterable<String> it2is(Iterable<Text> vs){
		List<String> ls = new ArrayList<String>();
		Iterator<Text> it = vs.iterator();
		while(it.hasNext()){
			ls.add(it.next().toString());
		}
		return ls;
	}
	/**
	 * reduce function in map-reduce mode
	 * set baseOutputPath to ETLCmd.SINGLE_TABLE for single table
	 * set newValue to null, if output line results
	 * @return list of newKey, newValue, baseOutputPath
	 */
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		Iterable<String> is = it2is(values);
		List<Tuple2<String, String>> kvlist = kvs2kvlist(key.toString(), is);
		for (Tuple2<String,String> kv:kvlist){
			mos.write(kv._2, null, kv._1);
		}
		return null;
	}
	
	@Override
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc){
		JavaPairRDD<String, String> mapret = input.mapToPair(new PairFunction<Tuple2<String,String>, String, String>(){
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				Tuple3<Integer, String, String> ike = kv2kv(t._1, t._2);
				return new Tuple2<String, String>(ike._2(), ike._3());
			}
		});
		return mapret.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>(){
			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
				return kvs2kvlist(t._1, t._2).iterator();
			}
		});
	}
}
