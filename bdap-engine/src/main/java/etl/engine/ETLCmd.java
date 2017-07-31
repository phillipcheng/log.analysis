package etl.engine;

import java.io.Serializable;
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

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import etl.cmd.SchemaETLCmd;
import etl.engine.types.DataType;
import etl.engine.types.InputFormatType;
import etl.engine.types.MRMode;
import etl.engine.types.ProcessMode;
import etl.input.CombineWithFileNameTextInputFormat;
import etl.util.ConfigKey;
import etl.util.SchemaUtils;
import etl.util.ScriptEngineUtil;
import etl.util.StringUtil;
import etl.util.VarDef;
import etl.util.VarType;

import scala.Tuple2;
import scala.Tuple3;

public abstract class ETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(ETLCmd.class);
	
	public static final String RESULT_KEY_LOG="log";
	public static final String RESULT_KEY_OUTPUT_LINE="lineoutput";
	public static final String RESULT_KEY_OUTPUT_TUPLE2="mapoutput";
	
	public static final String sys_cfgkey_use_keyvalue="use.keyvalue";
	//cfgkey
	public static final @ConfigKey(type=String[].class) String cfgkey_vars = "vars";
	public static final @ConfigKey(type=Boolean.class) String cfgkey_skip_header="skip.header";
	public static final @ConfigKey String cfgkey_skip_header_exp="skip.header.exp";
	public static final @ConfigKey String cfgkey_file_table_map="file.table.map";
	public static final @ConfigKey String cfgkey_key_value_sep="key.value.sep";//key value sep
	public static final @ConfigKey String cfgkey_csv_value_sep="input.delimiter";//csv value sep
	public static final @ConfigKey String cfgkey_input_endwith_delimiter="input.endwithcomma";//ending delimiter
	public static final @ConfigKey String cfgkey_record_type="record.type";//override what defined on the input data
	public static final @ConfigKey String cfgkey_input_format="input.format";//override what defined on the input data
	public static final @ConfigKey String cfgkey_path_filters="path.filters";//filter the input files for spark
	public static final @ConfigKey(type=etl.engine.types.OutputFormat.class) String cfgkey_output_file_format="output.file.format";
	
	//system variables
	public static final String VAR_NAME_TABLE_NAME="tablename";
	public static final String VAR_NAME_FILE_NAME="filename";
	public static final String VAR_NAME_PATH_NAME="pathname";//including filename
	
	public static final String DEFAULT_KEY_VALUE_SEP="\t";
	public static final String DEFAULT_CSV_VALUE_SEP=",";
	public static final String SINGLE_TABLE="singleTable";
	public static final String COLUMN_PREFIX="c";
	
	protected String wfName;//wf template name
	protected String wfid;
	protected String staticCfg;
	protected String defaultFs;
	protected String[] otherArgs;
	private String prefix;
	
	protected boolean skipHeader=false;
	protected transient CompiledScript skipHeaderCS;
	protected String strFileTableMap;
	protected transient CompiledScript expFileTableMap;
	protected String keyValueSep=DEFAULT_KEY_VALUE_SEP;
	protected String csvValueSep=DEFAULT_CSV_VALUE_SEP;
	protected boolean inputEndwithDelimiter=false;
	protected DataType recordType = null;
	protected InputFormatType inputFormatType = null;
	protected String[] pathFilters = null;
	protected transient List<Pattern> pathFilterExps = null;
	
	
	protected transient FileSystem fs;
	private transient PropertiesConfiguration pc;

	private transient Configuration conf;
	
	private ProcessMode pm = ProcessMode.Single;
	private MRMode mrMode = MRMode.line;
	private boolean sendLog = true;//command level send log flag
	
	//system variable map
	public static final String VAR_WFID="WFID"; //string type 
	public static final String VAR_FIELDS="fields"; //string[] type
	private transient Map<String, Object> systemVariables = null;
	
	public ETLCmd(){
	}
	
	public ETLCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
	}
	
	public ETLCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public ETLCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public ETLCmd(String wfName, String wfid, String staticCfg, String defaultFs){
		init(wfName, wfid, staticCfg, null, defaultFs, null, ProcessMode.Single);
	}
	
	public String toString(){
		return String.format("wfName:%s, cfg:%s", this.wfName, this.staticCfg);
	}
	
	/**
	 * @param wfName
	 * @param wfid
	 * @param staticCfg
	 * @param prefix
	 * @param defaultFs
	 * @param otherArgs
	 */
	public void init(String wfName, String wfid, String staticCfg, String prefix, 
			String defaultFs, String[] otherArgs, ProcessMode pm) {
		this.wfName = wfName;
		this.wfid = wfid;
		this.staticCfg = staticCfg;
		this.defaultFs = defaultFs;
		this.otherArgs = otherArgs;
		this.prefix = prefix;
		this.pm = pm;
		logger.info(String.format("wfName:%s, wfid:%s, cfg:%s, prefix:%s, defaultFs:%s, otherArgs:%s, processMode:%s", 
				wfName, wfid, staticCfg, prefix, defaultFs, otherArgs!=null?Arrays.asList(otherArgs):"null", pm));
		try {
			String fs_key = "fs.defaultFS";
			conf = new Configuration();
			if (defaultFs!=null){
				conf.set(fs_key, defaultFs);
			}
			this.fs = FileSystem.get(conf);
		}catch(Exception e){
			logger.error("", e);
		}
		pc = EngineUtil.getInstance().getMergedPC(staticCfg);
		keyValueSep = pc.getString(cfgkey_key_value_sep, null);
		systemVariables = new HashMap<String, Object>();
		systemVariables.put(VAR_WFID, wfid);
		systemVariables.put(EngineUtil.key_defaultfs, fs);
		String[] varnames = pc.getStringArray(cfgkey_vars);
		if (varnames!=null){
			for (String varname: varnames){
				String exp = pc.getString(varname);
				logger.info(String.format("global var:%s:%s", varname, exp));
				Object value = ScriptEngineUtil.eval(exp, VarType.OBJECT, systemVariables);
				systemVariables.put(varname, value);
			}
		}
		skipHeader =getCfgBoolean(cfgkey_skip_header, false);
		String skipHeaderExpStr = getCfgString(cfgkey_skip_header_exp, null);
		if (skipHeaderExpStr!=null){
			this.skipHeaderCS = ScriptEngineUtil.compileScript(skipHeaderExpStr);
		}
		strFileTableMap = getCfgString(cfgkey_file_table_map, null);
		logger.info(String.format("fileTableMap:%s", strFileTableMap));
		if (strFileTableMap!=null){
			expFileTableMap = ScriptEngineUtil.compileScript(strFileTableMap);
			logger.info(String.format("fileTableMapExp:%s", expFileTableMap));
		}
		String strRecordType = getCfgString(cfgkey_record_type, null);
		if (strRecordType!=null){
			recordType = DataType.valueOf(strRecordType);
		}
		String strInputFormatType = getCfgString(cfgkey_input_format, null);
		if (strInputFormatType!=null){
			inputFormatType = InputFormatType.valueOf(strInputFormatType);
		}
		this.csvValueSep=getCfgString(cfgkey_csv_value_sep, DEFAULT_CSV_VALUE_SEP);
		this.keyValueSep=getCfgString(cfgkey_key_value_sep, DEFAULT_KEY_VALUE_SEP);
		this.inputEndwithDelimiter=getCfgBoolean(cfgkey_input_endwith_delimiter, false);
		pathFilters = getCfgStringArray(cfgkey_path_filters);
		pathFilterExps = new ArrayList<Pattern>();
		for (String pathFilter:pathFilters){
			pathFilterExps.add(Pattern.compile(StringUtil.convertGlobToRegEx(pathFilter)));
		}
	}
	
	public void init(){
		if (fs==null){
			init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		}
	}
	
/*************
 * Overridable methods
 ***********/
	//in MapReduce Mode, do I have the reduce phase?
	public boolean hasReduce(){
		return true;
	}
	
	//will be overriden by schema-etl-cmd
	public String mapKey(String key){
		return key;
	}
	
	//using sparkSql to process, otherwise do default spark map reduce way
	public boolean useSparkSql(){
		return false;
	}
	
	public JavaPairRDD<String,String> dataSetProcess(JavaSparkContext jsc, SparkSession spark, JavaPairRDD<String, Row> rowRDD){
		logger.error(String.format("Empty dataSetProcess impl!!! %s", this));
		return null;
	}
	
	//base map method sub class needs to implement, for both mapreduce and spark
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception{
		logger.error(String.format("Empty flatMapToPair impl!!! %s", this));
		return null;
	}
	
	//base reduce method sub class needs to implement, for both mapreduce and spark
	//k,v,baseoutput
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<? extends Object> it, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		logger.error(String.format("Empty reduceByKey impl!!! %s", this));
		return null;
	}
	
	public static JavaPairRDD<String, Row> filterPairRDDRowNot(JavaPairRDD<String,Row> input, String key){
		return input.filter(new Function<Tuple2<String,Row>, Boolean>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Row> v1) throws Exception {
				if (key.equals(v1._1)){
					return false;
				}else{
					return true;
				}
			}
		});
	}
	
	public static JavaRDD<Row> filterPairRDDRow(JavaPairRDD<String,Row> input, String key){
		return input.filter(new Function<Tuple2<String,Row>, Boolean>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Row> v1) throws Exception {
				if (key.equals(v1._1)){
					return true;
				}else{
					return false;
				}
			}
		}).map(new Function<Tuple2<String, Row>, Row>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Row call(Tuple2<String, Row> v1) throws Exception {
				return v1._2;
			}
		});
	}
	
	public static Class<? extends InputFormat<LongWritable, Text>> getInputFormat(InputFormatType ift){
		if (InputFormatType.Line == ift){
			return org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class;
		}else if (InputFormatType.Text == ift){
			return org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class;
		}else if (InputFormatType.SequenceFile == ift){
			return etl.input.LTSequenceFileInputFormat.class;
		}else if (InputFormatType.XML == ift){
			return etl.input.XmlInputFormat.class;
		}else if (InputFormatType.CombineXML == ift){
			return etl.input.CombineXmlInputFormat.class;
		}else if (InputFormatType.StaXML == ift){
			return etl.input.StandardXmlInputFormat.class;
		}else if (InputFormatType.CombineWithFileNameText == ift){
			return etl.input.CombineWithFileNameTextInputFormat.class;
		}else if (InputFormatType.FileName == ift){
			return etl.input.FilenameInputFormat.class;
		}else if (InputFormatType.CombineFileName == ift){
			return etl.input.CombineFileNameInputFormat.class;
		}else{
			logger.error(String.format("inputformat:%s not supported", ift));
			return null;
		}
	}
	
	private List<Tuple2<String,String>> preprocess(Tuple2<String, String> kv, InputFormatType ift){
		List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
		if (InputFormatType.SequenceFile == ift){
			ret.addAll(processSequenceFile(null, kv._2.toString()));
		}else if (ift == InputFormatType.CombineWithFileNameText){
			String[] newkv = kv._2.split(CombineWithFileNameTextInputFormat.filename_value_sep, 2);
			String key = newkv[0];
			if (key!=null) key = mapKey(key);
			ret.add(new Tuple2<String,String>(key, newkv[1]));
		}else if (ift==InputFormatType.FileName){
			String key = mapKey(kv._2.toString());
			ret.add(new Tuple2<String,String>(key, kv._2));
		}else{
			String key = kv._1;
			if (key!=null) key = mapKey(key);
			ret.add(new Tuple2<String,String>(key, kv._2));
		}
		logger.debug(String.format("after preprocess:%s", ret));
		return ret;
	}
	
	//Files to KV
	public JavaPairRDD<String, String> sparkProcessFilesToKV(JavaPairRDD<String, String> inputfiles, JavaSparkContext jsc, InputFormatType ift, SparkSession spark){
		Class<? extends InputFormat<LongWritable, Text>> inputFormatClass = getInputFormat(ift);
		final InputFormatType realIft = (ift==InputFormatType.Text?InputFormatType.CombineWithFileNameText:ift);
		if (ift==InputFormatType.Text){
			inputFormatClass = CombineWithFileNameTextInputFormat.class;
		}
		copyConf();
		JobConf jobConf = new JobConf(this.getHadoopConf());
		for (Tuple2<String,String> file:inputfiles.collect()){
			logger.info(String.format("process path:%s", file));
			FileInputFormat.addInputPath(jobConf, new Path(file._2));
		}
		JavaPairRDD<LongWritable, Text> content = jsc.newAPIHadoopRDD(jobConf, inputFormatClass, LongWritable.class, Text.class);
		JavaPairRDD<String, String> tprdd = null;
		if (ift==InputFormatType.XML || ift==InputFormatType.CombineXML){
			tprdd = content.mapToPair(new PairFunction<Tuple2<LongWritable, Text>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, String> call(Tuple2<LongWritable, Text> t) throws Exception {
					return new Tuple2<String,String>(null, t._2.toString());
				}
				
			});
		}else{
			tprdd = content.flatMapToPair(new PairFlatMapFunction<Tuple2<LongWritable, Text>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<LongWritable, Text> t) throws Exception {
					init();
					List<Tuple2<String,String>> ret = new ArrayList<Tuple2<String,String>>();
					if (skipHeader && t._1.get()==0){
						return ret.iterator();
					}else{
						return preprocess(new Tuple2<String,String>(null, t._2.toString()), realIft).iterator();
					}
				}
			});
		}
		return sparkProcessKeyValue(tprdd, jsc, null, spark);
	}
	
	//KV to KV
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc, 
			InputFormatType ift, SparkSession spark){
		JavaPairRDD<String,String> processedInput = input;
		/*
		//file filter is not needed for spark
		logger.info("pathFilter:" + Arrays.asList(pathFilters));
		if (pathFilters!=null && pathFilters.length>0){
			processedInput = input.filter(new Function<Tuple2<String,String>, Boolean>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					init();
					for (Pattern p: pathFilterExps){
						if (p.matcher(v1._1).matches()){
							return true;
						}
					}
					return false;
				}
			});
		}
		//to be removed
		if (processedInput.count()==0){
			logger.error(String.format("no input after filter:%s", Arrays.asList(pathFilters)));
		}
		*/
		if (ift!=null){
			processedInput = processedInput.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					init();
					return preprocess(t, ift).iterator();
				}
			});
		}
		if (this.useSparkSql()){
			if (this instanceof SchemaETLCmd){
				SchemaETLCmd scmd = (SchemaETLCmd) this;
				JavaPairRDD<String, Row> rowRDD = processedInput.mapToPair(new PairFunction<Tuple2<String,String>, String, Row>(){
					private static final long serialVersionUID = 1L;
					@Override
					  public Tuple2<String, Row> call(Tuple2<String, String> t) throws Exception {
						String tn = t._1;
						if (scmd.getLogicSchema()==null){
							tn = ETLCmd.SINGLE_TABLE;
						}
						Object[] attributes = SchemaUtils.convertFromStringValues(
								scmd.getLogicSchema(), tn, t._2, csvValueSep, 
								inputEndwithDelimiter, skipHeader);
					    return new Tuple2<String, Row>(tn, RowFactory.create(attributes));
					  }
				});
				return dataSetProcess(jsc, spark, rowRDD);
			}else{
				logger.error(String.format("for spark sql usage must be subclass of SchemaCmd"));
				return null;
			}
		}else{
			JavaPairRDD<String, String> csvgroup = processedInput.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					init();
					List<Tuple2<String, String>> ret1 = flatMapToPair(t._1, t._2, null);
					//debug
					logger.debug(String.format("flatMapToPair process: %s", t));
					logger.debug(String.format("flatMapToPair return: %s", ret1));
					if (ret1!=null) 
						return ret1.iterator();
					else 
						return new ArrayList<Tuple2<String,String>>().iterator();
				}
			});
			if (this.hasReduce()){
				JavaPairRDD<String, String> csvaggr = csvgroup.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
						List<Tuple3<String, String, String>> t3l = reduceByKey(t._1, t._2, null, null);
						List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String,String>>();
						for (Tuple3<String, String, String> t3 : t3l){
							if (t3._2()!=null){
								ret.add(new Tuple2<String, String>(t3._3(), t3._1() + csvValueSep + t3._2()));
							}else{
								ret.add(new Tuple2<String, String>(t3._3(), t3._1()));
							}
						}
						return ret.iterator();
					}
				});
				return csvaggr;
			}else{
				return csvgroup;
			}
		}
	}
	
	/**
	 * map function in map-only or map-reduce mode, for map mode: output null for no key or value
	 * @return map may contains following key:
	 * ETLCmd.RESULT_KEY_LOG: list of String user defined log info
	 * ETLCmd.RESULT_KEY_OUTPUT: list of String output
	 * ETLCmd.RESULT_KEY_OUTPUT_MAP: list of Tuple2<key, value>
	 * null, if in the mapper it write to context directly for performance
	 * in the value map, if it contains only 1 value, the key should be ETLCmd.RESULT_KEY_OUTPUT
	 */
	public Map<String, Object> mapProcess(long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception {
		if (skipHeader && offset==0) {
			logger.info("skip header:" + row);
			return null;
		}
		String[] kv=null;
		if (context.getConfiguration().get(sys_cfgkey_use_keyvalue, null)!=null){
			kv = row.split(keyValueSep, 2);//try to split to key value, if failed use file name as key
		}
		String tfName = null;
		if (kv!=null && kv.length==2){
			tfName = kv[0];
			row = kv[1];
		}else{
			tfName = getTableNameSetFileNameByContext(context);
		}
		
		List<Tuple2<String, String>> it = flatMapToPair(tfName, row, context);
		if (it!=null){
			for (Tuple2<String,String> t : it){
				if (t._2!=null){
					context.write(new Text(t._1), new Text(t._2));
				}else{//for no reduce job, value can be null
					context.write(new Text(t._1), null);
				}
			}
		}
		return null;
	}
	
	/**
	 * single thread process
	 * @return list of String user defined log info
	 */
	public List<String> sgProcess() throws Exception{
		logger.error(String.format("Empty sgProcess impl!!! %s", this));
		return null;
	}
	
	//for long running command
	public void close(){
	}

/*************
 * Utility methods
 ****/		
	public VarDef[] getCfgVar(){
		return new VarDef[]{new VarDef(cfgkey_vars, VarType.STRINGLIST)};
	}
	
	public VarDef[] getSysVar(){
		return new VarDef[]{
				new VarDef(VAR_NAME_TABLE_NAME, VarType.STRING), 
				new VarDef(VAR_NAME_FILE_NAME, VarType.STRING), 
				new VarDef(VAR_NAME_PATH_NAME, VarType.STRING)};
	}
	
	public Map<String, Object> getSystemVariables(){
		return systemVariables;
	}
	
	//copy propertiesConf to jobConf
	public void copyConf(){
		Iterator<String> it = pc.getKeys();
		while (it.hasNext()){
			String key = it.next();
			String value = pc.getString(key);
			logger.debug(String.format("copy property:%s:%s", key, value));
			this.conf.set(key, value);
		}
	}
	
	public void copyConf(Map<String, String> inPc){
		for (String key: inPc.keySet()){
			String value = inPc.get(key);
			this.conf.set(key, value);
		}
	}
	
	public String getTableNameSetPathFileName(String pathName){
		getSystemVariables().put(VAR_NAME_PATH_NAME, pathName);
		int lastSep = pathName.lastIndexOf("/");
		String fileName = pathName.substring(lastSep+1);
		getSystemVariables().put(VAR_NAME_FILE_NAME, fileName);
		if (expFileTableMap!=null){
			return ScriptEngineUtil.eval(expFileTableMap, this.getSystemVariables());
		}else{
			return pathName;
		}
	}
	
	public String getTableNameSetFileNameByContext(Mapper<LongWritable, Text, Text, Text>.Context context){
		if (context.getInputSplit() instanceof FileSplit){
			Path path=((FileSplit) context.getInputSplit()).getPath();
			getSystemVariables().put(VAR_NAME_PATH_NAME, path.toString());
			String inputFileName = path.getName();
			this.getSystemVariables().put(VAR_NAME_FILE_NAME, inputFileName);
			String tableName = inputFileName;
			if (expFileTableMap!=null){
				tableName = ScriptEngineUtil.eval(expFileTableMap, this.getSystemVariables());
			}
			return tableName;
		}else{
			return null;
		}
	}
	
	//if pathName is null, pathName is the 1st line of the content
	private List<Tuple2<String, String>> processSequenceFile(String pathName, String content){
		List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
		String[] lines = content.split("\n");
		if (pathName==null){
			pathName = lines[0];
		}
		String key = mapKey(pathName);
		boolean skipFirstLine = skipHeader;
		if (!skipHeader && skipHeaderCS!=null){
			boolean skip = (boolean) ScriptEngineUtil.evalObject(skipHeaderCS, getSystemVariables());
			skipFirstLine = skipFirstLine || skip;
		}
		int start=1;
		if (skipFirstLine) start++;
		for (int i=start; i<lines.length; i++){
			String line = lines[i];
			ret.add(new Tuple2<String, String>(key, line));
		}
		return ret;
	}

/*********
 * Cfg operations
 */
	public boolean cfgContainsKey(String key){
		String realKey = key;
		if (prefix!=null){
			realKey = prefix+"."+key;
		}
		return pc.containsKey(realKey);
	}
	
	public Iterator<String> getCfgKeys(){
		if (prefix!=null){
			String realPrefix = prefix+".";
			return pc.getKeys(realPrefix);
		}else{
			return pc.getKeys();
		}
	}
	
	public Object getCfgProperty(String key){
		String realKey = key;
		if (prefix!=null){
			realKey = prefix+"."+key;
		}
		if (pc.containsKey(realKey)){
			return pc.getProperty(realKey);
		}else{
			return pc.getProperty(key);
		}
	}
	
	public boolean getCfgBoolean(String key, boolean defaultValue){
		String realKey = key;
		if (prefix!=null){
			realKey = prefix+"."+key;
		}
		if (pc.containsKey(realKey)){
			return pc.getBoolean(realKey, defaultValue);
		}else{
			return pc.getBoolean(key, defaultValue);
		}
	}
	
	public int getCfgInt(String key, int defaultValue){
		String realKey = key;
		if (prefix!=null){
			realKey = prefix+"."+key;
		}
		if (pc.containsKey(realKey)){
			return pc.getInt(realKey, defaultValue);
		}else{
			return pc.getInt(key, defaultValue);
		}
	}
	
	public String getCfgString(String key, String defaultValue){
		String realKey = key;
		if (prefix!=null){
			realKey = prefix+"."+key;
		}
		pc.setDelimiterParsingDisabled(true);
		String v=null;
		if (pc.containsKey(realKey)){
			v = pc.getString(realKey, defaultValue);
		}else{
			v = pc.getString(key, defaultValue);
		}
		pc.setDelimiterParsingDisabled(false);
		return v;
	}
	
	//return 0 length array if not found
	public String[] getCfgStringArray(String key){
		String realKey = key;
		if (prefix!=null){
			realKey = prefix+"."+key;
		}
		if (pc.containsKey(realKey)){
			return pc.getStringArray(realKey);
		}else{
			return pc.getStringArray(key);
		}
	}
	
/**********
 * setter and getter
 */
	public Configuration getHadoopConf(){
		return conf;
	}
	
	public MRMode getMrMode() {
		return mrMode;
	}

	public void setMrMode(MRMode mrMode) {
		this.mrMode = mrMode;
	}
	
	public String getWfid(){
		return wfid;
	}
	
	public void setWfid(String wfid) {
		this.wfid = wfid;
	}

	public PropertiesConfiguration getPc() {
		return pc;
	}

	public void setPc(PropertiesConfiguration pc) {
		this.pc = pc;
	}

	public boolean isSendLog() {
		return sendLog;
	}

	public void setSendLog(boolean sendLog) {
		this.sendLog = sendLog;
	}

	public FileSystem getFs() {
		return fs;
	}

	public void setFs(FileSystem fs) {
		this.fs = fs;
	}

	public String getDefaultFs() {
		return defaultFs;
	}

	public void setDefaultFs(String defaultFs) {
		this.defaultFs = defaultFs;
	}

	public String getWfName() {
		return wfName;
	}

	public void setWfName(String wfName) {
		this.wfName = wfName;
	}
}
