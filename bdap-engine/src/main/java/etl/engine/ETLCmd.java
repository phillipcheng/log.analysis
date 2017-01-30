package etl.engine;

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

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import etl.cmd.SchemaETLCmd;
import bdap.util.EngineConf;
import etl.input.FilenameInputFormat;
import etl.util.ConfigKey;
import etl.util.SchemaUtils;
import etl.util.ScriptEngineUtil;
import etl.util.SparkUtil;
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
	}
	
	public void init(){
		if (fs==null){
			init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		}
	}
	
	public void reinit(){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
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
	
	public JavaPairRDD<String,String> dataSetProcess(JavaSparkContext jsc, SparkSession spark, int singleTableColNum){
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
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<String> it, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		logger.error(String.format("Empty reduceByKey impl!!! %s", this));
		return null;
	}
	
	//K to KV
	public JavaPairRDD<String, String> sparkProcessV2KV(JavaRDD<String> input, JavaSparkContext jsc, 
			InputFormatType ift, SparkSession spark){
		JavaPairRDD<String, String> pairs = input.mapToPair(new PairFunction<String, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				return new Tuple2<String, String>(null, t);
			}
		});
		return sparkProcessKeyValue(pairs, jsc, ift, spark);
	}
	
	//KV to KV
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc, 
			InputFormatType ift, SparkSession spark){
		JavaPairRDD<String,String> processedInput = input;
		if (ift!=null){
			processedInput = input.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					init();
					List<Tuple2<String, String>> linesInput = new ArrayList<Tuple2<String,String>>();
					if (InputFormatType.SequenceFile == ift){
						linesInput.addAll(processSequenceFile(t._1, t._2));
					}else{
						linesInput.add(t);
					}
					List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String,String>>();
					for (Tuple2<String, String> lineInput:linesInput){
						String key = lineInput._1;
						if (key!=null && ift!=null){
							//called directly from data, need map, otherwise called from sparkProcessFilesToKV, do not need to mapkey again
							key = mapKey(key);
						}
						ret.add(new Tuple2<String,String>(key, lineInput._2));
					}
					return ret.iterator();
				}
			});
		}
		if (this.useSparkSql()){
			if (this instanceof SchemaETLCmd){
				int singleTableColNum=0;
				SchemaETLCmd scmd = (SchemaETLCmd) this;
				if (scmd.getLogicSchema()==null){//schema less, each field is treated as string typed
					JavaRDD<String> frdd = processedInput.values();
					singleTableColNum = frdd.top(1).get(0).split(csvValueSep,-1).length;
					StructType st = new StructType();
					for (int i=0; i<singleTableColNum; i++){
						st = st.add(DataTypes.createStructField(COLUMN_PREFIX+i, DataTypes.StringType, true));
					}
					char delimiter = csvValueSep.charAt(0);
					//no schema, then all input goes to 1 table SINGLE_TABLE
					JavaRDD<Row> rowRDD = frdd.map(new Function<String, Row>() {
						private static final long serialVersionUID = 1L;
						@Override
						  public Row call(String record) throws Exception {
							//String[] fvs = record.split(csvValueSep, -1);
							CSVParser csvParser=CSVParser.parse(record, CSVFormat.DEFAULT.withTrim().
									withDelimiter(delimiter).withTrailingDelimiter(inputEndwithDelimiter).
									withSkipHeaderRecord(skipHeader));
							CSVRecord csvRecord = csvParser.getRecords().get(0);
							String[] fvs = new String[csvRecord.size()];
							for (int i=0; i<fvs.length; i++){
								fvs[i]=csvRecord.get(i);
							}
						    return RowFactory.create(fvs);
						  }
						});
					Dataset<Row> dfr = spark.createDataFrame(rowRDD, st);
					dfr.createOrReplaceTempView(SINGLE_TABLE);
				}else{
					List<String> keys = processedInput.keys().distinct().collect();
					for (String key: keys){
						JavaRDD<String> frdd = SparkUtil.filterPairRDD(processedInput, key);
						StructType st = scmd.getSparkSqlSchema(key);
						if (st!=null){
							JavaRDD<Row> rowRDD = frdd.map(new Function<String, Row>() {
								private static final long serialVersionUID = 1L;
								@Override
								  public Row call(String record) throws Exception {
									Object[] attributes = SchemaUtils.convertFromStringValues(
											scmd.getLogicSchema(), key, record, csvValueSep, 
											inputEndwithDelimiter, skipHeader);
								    return RowFactory.create(attributes);
								  }
								});
							Dataset<Row> dfr = spark.createDataFrame(rowRDD, st);
							dfr.createOrReplaceTempView(key);
						}else{
							logger.error("schema not found for %s", key);
							return null;
						}
					}
				}
				return dataSetProcess(jsc, spark, singleTableColNum);
			}else{
				logger.error(String.format("expected SchemaETLCmd subclasses. %s", this.getClass().getName()));
				return null;
			}
		}else{
			JavaPairRDD<String, String> csvgroup = processedInput.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					init();
					List<Tuple2<String, String>> ret1 = flatMapToPair(t._1, t._2, null);
					if (ret1!=null) return ret1.iterator();
					else return null;
				}
			}).filter(new Function<Tuple2<String, String>, Boolean>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					if (v1==null){
						return false;
					}else{
						return true;
					}
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
	
	//Files to KV
	public JavaPairRDD<String, String> sparkProcessFilesToKV(JavaRDD<String> inputfiles, JavaSparkContext jsc, InputFormatType ift, SparkSession spark){
		JavaPairRDD<String, String> prdd = null;
		Class<? extends InputFormat<LongWritable, Text>> inputFormatClass = getInputFormat(ift);
		for (String file:inputfiles.collect()){
			copyConf();
			JobConf jobConf = new JobConf(this.getHadoopConf());
			logger.info(String.format("process path:%s", file));
			FileInputFormat.addInputPath(jobConf, new Path(file));
			JavaPairRDD<LongWritable, Text> content = jsc.newAPIHadoopRDD(jobConf, inputFormatClass, LongWritable.class, Text.class);
			JavaPairRDD<String, String> tprdd = content.flatMapToPair(new PairFlatMapFunction<Tuple2<LongWritable, Text>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<LongWritable, Text> t) throws Exception {
					init();
					List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
					if (SequenceFileInputFormat.class.isAssignableFrom(inputFormatClass)){
						ret.addAll(processSequenceFile(null, t._2.toString()));
					}else{
						String key = null;
						if (inputFormatClass.isAssignableFrom(FilenameInputFormat.class)){
							key = mapKey(t._2.toString());
						}else{
							key = mapKey(file);
						}
						if (!(skipHeader && t._1.get()==0)){
							ret.add(new Tuple2<String, String>(key, t._2.toString()));
						};
					}
					return ret.iterator();
				}
				
			});
			if (prdd==null){
				prdd = tprdd;
			}else{
				prdd = prdd.union(tprdd);
			}
		}
		return sparkProcessKeyValue(prdd, jsc, null, spark);
	}
	
	//V to V
	public JavaRDD<String> sparkProcess(JavaRDD<String> input, JavaSparkContext jsc, InputFormatType ift, SparkSession spark){
		return input.flatMap(new FlatMapFunction<String,String>(){
			@Override
			public Iterator<String> call(String t) throws Exception {
				List<Tuple2<String,String>> retlist = flatMapToPair(null, t, null);
				List<String> keylist = new ArrayList<String>();
				if (retlist!=null){
					for (Tuple2<String,String> ret:retlist){
						keylist.add(ret._1);
					}
				}
				return keylist.iterator();
			}
		});
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
	 * reduce function in map-reduce mode
	 * return List of [newkey, newValue, baseOutputPath]
	 * return null, means done in the subclass
	 * set baseOutputPath to ETLCmd.SINGLE_TABLE for single table
	 * set newValue to null, if output line results
	 * @return list of newKey, newValue, baseOutputPath
	 */
	public List<String[]> reduceProcess(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		Iterable<String> strValues = itTois(values);
		List<Tuple3<String, String, String>> retList = this.reduceByKey(key.toString(), strValues, context, mos);
		List<String[]> retStringlist = new ArrayList<String[]>();
		for(Tuple3<String, String, String> ret:retList){
			retStringlist.add(new String[]{ret._1(), ret._2(), ret._3()});
		}	
		return retStringlist;
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

	public static Iterable<String> itTois(Iterable<Text> values){
		List<String> svalues = new ArrayList<String>();
		Iterator<Text> vit = values.iterator();
		while (vit.hasNext()){
			svalues.add(vit.next().toString());
		}
		return svalues;
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
