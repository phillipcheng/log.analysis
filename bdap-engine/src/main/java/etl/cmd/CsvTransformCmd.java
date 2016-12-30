package etl.cmd;

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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import bdap.util.Util;
import etl.cmd.transform.ColOp;
import etl.engine.ETLCmd;
import etl.engine.MRMode;
import etl.engine.OutputType;
import etl.engine.ProcessMode;
import etl.input.CombineWithFileNameTextInputFormat;
import etl.util.FieldType;
import etl.util.ScriptEngineUtil;

import scala.Tuple2;
import scala.Tuple3;

public class CsvTransformCmd extends SchemaETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(CsvTransformCmd.class);
	
	//cfgkey
	public static final String cfgkey_input_endwithcomma="input.endwithcomma";
	public static final String cfgkey_input_endwithcomma_exp="input.endwithcomma.exp";
	public static final String cfgkey_row_validation="row.validation";
	public static final String cfgkey_col_op="col.op";
	public static final String cfgkey_old_talbe="old.table";
	public static final String cfgkey_add_fields="add.fields";
	
	private boolean inputEndWithComma=false;
	private transient CompiledScript rowValidation;
	private transient CompiledScript inputEndWithCommaCS;
	private String oldTable;
	
	private transient List<String> addFieldsNames;
	private transient List<String> addFieldsTypes;
	private transient List<ColOp> colOpList;
	
	//
	private transient List<String> tableAttrs = null;
	private transient Map<String, Integer> nameIdxMap = null;
	
	public CsvTransformCmd(){
		super();
	}
	
	public CsvTransformCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public CsvTransformCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public CsvTransformCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		this.setMrMode(MRMode.line);
		String rowValidationStr;
		String inputEndWithCommaStr;
		inputEndWithComma = super.getCfgBoolean(cfgkey_input_endwithcomma, false);
		rowValidationStr = super.getCfgString(cfgkey_row_validation, null);
		inputEndWithCommaStr = super.getCfgString(cfgkey_input_endwithcomma_exp, null);
		oldTable = super.getCfgString(cfgkey_old_talbe, null);
		addFieldsNames = new ArrayList<String>();
		addFieldsTypes = new ArrayList<String>();
		colOpList = new ArrayList<ColOp>();
		
		// compile the expression to speedup
		if (rowValidationStr != null && rowValidationStr.length() > 0)
			rowValidation = ScriptEngineUtil.compileScript(rowValidationStr);
		
		// compile the expression to speedup
		if (inputEndWithCommaStr != null && inputEndWithCommaStr.length() > 0)
			inputEndWithCommaCS = ScriptEngineUtil.compileScript(inputEndWithCommaStr);
		
		//for dynamic trans
		if (this.logicSchema!=null){
			nameIdxMap = new HashMap<String, Integer>();
			tableAttrs = logicSchema.getAttrNames(this.oldTable);
			if (tableAttrs==null){
				logger.error(String.format("table %s not exist or specified.", this.oldTable));
			}else {
				for (int i=0; i<tableAttrs.size(); i++){
					nameIdxMap.put(tableAttrs.get(i), i);
				}
			}
		}
		String[] colops = super.getCfgStringArray(cfgkey_col_op);
		for (String colop:colops){
			ColOp co = new ColOp(colop, nameIdxMap);
			colOpList.add(co);
		}
		String[] nvs = super.getCfgStringArray(cfgkey_add_fields);
		if (nvs!=null){
			for (String nv:nvs){
				String[] nva = nv.split("\\:",2);
				addFieldsNames.add(nva[0]);
				addFieldsTypes.add(nva[1]);
			}
		}
	}
	
	@Override
	public List<String> sgProcess(){
		List<String> attrs = logicSchema.getAttrNames(this.oldTable);
		List<String> addFns = new ArrayList<String>();
		List<FieldType> addFts = new ArrayList<FieldType>();
		for (int i=0; i<addFieldsNames.size(); i++){
			String fn = addFieldsNames.get(i);
			FieldType ft = new FieldType(addFieldsTypes.get(i));
			if (!attrs.contains(fn)){
				//update schema
				addFns.add(fn);
				addFts.add(ft);
			}
		}
		if (addFns.size()>0){
			Map<String, List<String>> attrsMap = new HashMap<String, List<String>>();
			Map<String, List<FieldType>> attrTypesMap =new HashMap<String, List<FieldType>>();
			attrsMap.put(oldTable, addFns);
			attrTypesMap.put(oldTable, addFts);
			return super.updateSchema(attrsMap, attrTypesMap);
		}else{
			return null;
		}
	}

	@Override
	public List<Tuple2<String, String>> flatMapToPair(String pathName, String value, Mapper<LongWritable, Text, Text, Text>.Context context){
		super.init();
		String row = value;
		String output="";
		
		//get all fiels
		List<String> items = new ArrayList<String>();
		items.addAll(Arrays.asList(row.split(",", -1)));
		
		super.getSystemVariables().put(ColOp.VAR_NAME_FIELDS, items.toArray(new String[0]));
		//process row validation
		if (rowValidation!=null){//put this 1st to improve performance
			Object valid = ScriptEngineUtil.evalObject(rowValidation, super.getSystemVariables());
			if (valid instanceof Boolean) {
				if (!(Boolean)valid) {
					logger.debug("invalid row:" + row);
					return null;
				}
			} /* If result is not boolean, the expression has no effect */
		}
		
		this.getSystemVariables().put(VAR_NAME_PATH_NAME, pathName);
		String tableName = getTableNameSetFileName(pathName);
		
		//set the fieldsMap
		Map<String, String> fieldMap = null;
		if (nameIdxMap!=null){
			fieldMap = new HashMap<String, String>();
			for (int i=0; i<tableAttrs.size(); i++){
				if (i<items.size()){
					fieldMap.put(tableAttrs.get(i), items.get(i));
				}else{
					fieldMap.put(tableAttrs.get(i), "");
				}
			}
		}
		//calculate whether use inputEndWithComma
		boolean hasEndComma=inputEndWithComma;
		if (inputEndWithCommaCS != null) {
			Object result = ScriptEngineUtil.evalObject(inputEndWithCommaCS, super.getSystemVariables());
			if (result instanceof Boolean && result!=null) {
				hasEndComma=(Boolean) result;
			} /* If result is not boolean, use the default */
		}
		//process input ends with comma
		if (hasEndComma){//remove the last empty item since row ends with comma
			items.remove(items.size()-1);
		}
		
		//process operation
		super.getSystemVariables().put(ColOp.VAR_NAME_FIELD_MAP, fieldMap);
		super.getSystemVariables().put(VAR_NAME_TABLE_NAME, tableName);
		for (ColOp co: colOpList){
			items = co.process(super.getSystemVariables(), items);
		}
		output = Util.getCsv(items, false);
		logger.debug(String.format("tableName:%s, output:%s", tableName, output));
		return Arrays.asList(new Tuple2<String, String>(tableName, output));
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) {
		Map<String, Object> retMap = new HashMap<String, Object>();
		
		try {
			if (context.getInputFormatClass().isAssignableFrom(SequenceFileInputFormat.class)){
				String[] lines = row.split("\n");
				String pathName = null;
				pathName = lines[0];
				boolean skipFirstLine = skipHeader;
				if (!skipHeader && skipHeaderCS!=null){
					this.getSystemVariables().put(VAR_NAME_PATH_NAME, pathName);
					getTableNameSetFileName(pathName);
					boolean skip = (boolean) ScriptEngineUtil.evalObject(skipHeaderCS, super.getSystemVariables());
					skipFirstLine = skipFirstLine || skip;
				}
				int start=1;
				if (skipFirstLine) start=2;
				List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
				for (int i=start; i<lines.length; i++){
					String line = lines[i];
					List<Tuple2<String, String>> ret = flatMapToPair(pathName, line, context);
					if (ret!=null) output.addAll(ret);
				}
				retMap.put(RESULT_KEY_OUTPUT_TUPLE2, output);
			}else{
				String pathName = null;
				//process skip header
				if (skipHeader && offset==0) {
					logger.info("skip header:" + row);
					return null;
				}
				if (context.getInputSplit() instanceof FileSplit){
					pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
				}else if (context.getInputSplit() instanceof CombineFileSplit){
					int idx = row.indexOf(CombineWithFileNameTextInputFormat.filename_value_sep);
					pathName = row.substring(0, idx);
					row = row.substring(idx+1);
				}else{
					logger.error(String.format("unsupported input split:%s", context.getInputSplit()));
				}
				//process skip header exp
				if (skipHeaderCS!=null && offset==0){
					this.getSystemVariables().put(VAR_NAME_PATH_NAME, pathName);
					getTableNameSetFileName(pathName);
					boolean skip = (boolean) ScriptEngineUtil.evalObject(skipHeaderCS, super.getSystemVariables());
					if (skip){
						logger.info("skip header:" + row);
						return null;
					}
				}
				retMap.put(RESULT_KEY_OUTPUT_TUPLE2, flatMapToPair(pathName, row, null));
			}
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
		return retMap;
	}
	
	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<String> values,
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos){
		//write output using the lastpart of the path name.
		String pathName = key.toString();
		int lastSep = pathName.lastIndexOf("/");
		String fileName = pathName.substring(lastSep+1);
		List<Tuple3<String, String, String>> ret = new ArrayList<Tuple3<String, String, String>>();	
		Iterator<String> it = values.iterator();
		while (it.hasNext()){
			String v = it.next().toString();
			if (super.getOutputType()==OutputType.multiple){
				ret.add(new Tuple3<String, String, String>(v, null, fileName.toString()));
			}else{
				ret.add(new Tuple3<String, String, String>(v, null, ETLCmd.SINGLE_TABLE));
			}
		}
		return ret;
	}
	
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		List<String> svalues = new ArrayList<String>();
		Iterator<Text> vit = values.iterator();
		while (vit.hasNext()){
			svalues.add(vit.next().toString());
		}
		List<String[]> ret = new ArrayList<String[]>();	
		List<Tuple3<String, String, String>> output = reduceByKey(key.toString(), svalues, context, mos);
		for (Tuple3<String, String, String> t: output){
			ret.add(new String[]{t._1(), t._2(), t._3()});
		}
		return ret;
	}

	public String getOldTable() {
		return oldTable;
	}

	public void setOldTable(String oldTable) {
		this.oldTable = oldTable;
	}
}
