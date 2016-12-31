package etl.engine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import etl.spark.SparkProcessor;
import etl.util.ScriptEngineUtil;
import etl.util.VarDef;
import etl.util.VarType;
import scala.Tuple2;
import scala.Tuple3;

public abstract class ETLCmd implements Serializable, SparkProcessor{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(ETLCmd.class);
	
	public static final String RESULT_KEY_LOG="log";
	public static final String RESULT_KEY_OUTPUT_LINE="lineoutput";
	public static final String RESULT_KEY_OUTPUT_TUPLE2="mapoutput";
	
	//cfgkey
	public static final String cfgkey_vars="vars";
	public static final String cfgkey_skip_header="skip.header";
	public static final String cfgkey_skip_header_exp="skip.header.exp";
	public static final String cfgkey_file_table_map="file.table.map";
	
	//system variables
	public static final String VAR_NAME_TABLE_NAME="tablename";
	public static final String VAR_NAME_FILE_NAME="filename";
	public static final String VAR_NAME_PATH_NAME="pathname";//including filename
	
	public static final String KEY_SEP=",";
	public static final String SINGLE_TABLE="singleTable";
	
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
		systemVariables = new HashMap<String, Object>();
		systemVariables.put(VAR_WFID, wfid);
		systemVariables.put(EngineUtil.key_defaultfs, EngineUtil.getInstance().getDefaultFs());
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
		
	}
	
	public void init(){
		if (fs==null){
			init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		}
	}
	
	public void reinit(){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
	}
	
	public Map<String, Object> getSystemVariables(){
		return systemVariables;
	}
	
	//copy propertiesConf to jobConf
	private void copyConf(){
		Iterator it = pc.getKeys();
		while (it.hasNext()){
			String key = (String) it.next();
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
	
	//will be overriden by schema-etl-cmd
	public String mapKey(String key){
		return key;
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
	
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception{
		logger.error("empty flatMapToPair impl, should not be invoked.");
		return null;
	}
	
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<String> it, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		logger.error("empty reduceByKey impl, should not be invoked.");
		return null;
	}
	
	@Override
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc){
		JavaPairRDD<String, String> csvgroup = input.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
				List<Tuple2<String, String>> ret = flatMapToPair(t._1, t._2, null);
				if (ret!=null){
					return ret.iterator();
				}else{
					return Collections.emptyIterator();
				}
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
		
		JavaPairRDD<String, String> csvaggr = csvgroup.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
				List<Tuple3<String, String, String>> t3l = reduceByKey(t._1, t._2, null, null);
				List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String,String>>();
				for (Tuple3<String, String, String> t3 : t3l){
					if (t3._2()!=null){
						ret.add(new Tuple2<String, String>(t3._3(), t3._1() + KEY_SEP + t3._2()));
					}else{
						ret.add(new Tuple2<String, String>(t3._3(), t3._1()));
					}
				}
				return ret.iterator();
			}
		});
		return csvaggr;
	}
	
	@Override
	public JavaRDD<String> sparkProcess(JavaRDD<String> input, JavaSparkContext jsc){
		logger.error("empty sparkProcess impl, should not be invoked.");
		return null;
	}
	
	@Override
	public JavaPairRDD<String, String> sparkProcessV2KV(JavaRDD<String> input, JavaSparkContext jsc){
		logger.error("empty sparkProcessV2KV impl, should not be invoked.");
		return null;
	}
	
	public JavaPairRDD<String, String> sparkProcessFilesToKV(JavaRDD<String> inputfiles, JavaSparkContext jsc, 
			Class<? extends InputFormat> inputFormatClass){
		JavaPairRDD<String, String> prdd = null;
		for (String file:inputfiles.collect()){
			copyConf();
			JobConf jobConf = new JobConf(this.getHadoopConf());
			FileInputFormat.addInputPath(jobConf, new Path(file));
			JavaPairRDD<LongWritable, Text> content = jsc.newAPIHadoopRDD(jobConf, inputFormatClass, LongWritable.class, Text.class);
			JavaPairRDD<String, String> tprdd = content.flatMapToPair(new PairFlatMapFunction<Tuple2<LongWritable, Text>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<LongWritable, Text> t) throws Exception {
					init();
					List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
					if (inputFormatClass.isAssignableFrom(SequenceFileInputFormat.class)){
						String row = t.toString();
						String[] lines = row.split("\n");
						String pathName = lines[0];
						String key = mapKey(pathName);
						boolean skipFirstLine = skipHeader;
						if (!skipHeader && skipHeaderCS!=null){
							boolean skip = (boolean) ScriptEngineUtil.evalObject(skipHeaderCS, getSystemVariables());
							skipFirstLine = skipFirstLine || skip;
						}
						int start=1;
						if (skipFirstLine) start=2;
						List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
						for (int i=start; i<lines.length; i++){
							String line = lines[i];
							ret.add(new Tuple2<String, String>(key, line));
						}
					}else{
						String key = mapKey(file);
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
		return sparkProcessKeyValue(prdd, jsc);
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
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		logger.error("empty map impl, should not be invoked.");
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
		logger.error("empty reduce impl, should not be invoked.");
		return null;
	}
	
	/**
	 * single thread process
	 * @return list of String user defined log info
	 */
	public List<String> sgProcess() throws Exception{
		logger.error("empty single impl, should not be invoked.");
		return null;
	}
	
	//for long running command
	public void close(){
	}
	
	//in MapReduce Mode, do I have the reduce phase?
	public boolean hasReduce(){
		return true;
	}
	
	public VarDef[] getCfgVar(){
		return new VarDef[]{new VarDef(cfgkey_vars, VarType.STRINGLIST)};
	}
	
	
	public VarDef[] getSysVar(){
		return new VarDef[]{
				new VarDef(VAR_NAME_TABLE_NAME, VarType.STRING), 
				new VarDef(VAR_NAME_FILE_NAME, VarType.STRING), 
				new VarDef(VAR_NAME_PATH_NAME, VarType.STRING)};
	}
	
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
	
	///
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
