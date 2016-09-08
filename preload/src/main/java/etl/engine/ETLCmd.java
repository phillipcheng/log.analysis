package etl.engine;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.util.Util;
import scala.Tuple2;
import scala.Tuple3;

public abstract class ETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = Logger.getLogger(ETLCmd.class);
	
	public static final String RESULT_KEY_LOG="log";
	public static final String RESULT_KEY_OUTPUT_LINE="lineoutput";
	public static final String RESULT_KEY_OUTPUT_TUPLE2="mapoutput";

	public static final String SINGLE_TABLE="single.table";
	
	protected String wfid;
	protected String staticCfg;
	protected String defaultFs;
	protected String[] otherArgs;
	
	protected transient FileSystem fs;
	protected transient PropertiesConfiguration pc;

	private transient Configuration conf;
	
	private ProcessMode pm = ProcessMode.SingleProcess;
	private MRMode mrMode = MRMode.file;
	private boolean sendLog = true;//command level send log flag
	
	//system variable map
	public static final String VAR_WFID="WFID"; //string type 
	public static final String VAR_FIELDS="fields"; //string[] type
	private transient Map<String, Object> systemVariables = null;
	
	public ETLCmd(){
	}
	
	public ETLCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfid, staticCfg, defaultFs, otherArgs);
	}
	
	public void init(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		this.wfid = wfid;
		this.staticCfg = staticCfg;
		this.defaultFs = defaultFs;
		this.otherArgs = otherArgs;
		logger.info(String.format("wfid:%s, cfg:%s, defaultFs:%s", wfid, staticCfg, defaultFs));
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
		pc = Util.getMergedPCFromDfs(fs, staticCfg);
		systemVariables = new HashMap<String, Object>();
		systemVariables.put(VAR_WFID, wfid);
	}
	
	public void init(){
		if (fs==null){
			init(wfid, staticCfg, defaultFs, otherArgs);
		}
	}
	
	public Map<String, Object> getSystemVariables(){
		return systemVariables;
	}
	
	/**
	 * used by spark driver
	 * @param key
	 * @param value
	 * @return key, value pairs
	 */
	public List<Tuple2<String, String>> flatMapToPair(String key, String value){
		logger.error("empty map impl, should not be invoked.");
		return null;
	}
	/**
	 * used by spark driver
	 * @param key
	 * @param it
	 * @return newKey, csv, entity/table name
	 */
	public Tuple3<String, String, String> reduceByKey(String key, Iterable<String> it){
		logger.error("empty map impl, should not be invoked.");
		return null;
	}
	
	/**
	 * map function in map-only or map-reduce mode, for map mode: output null for no key or value
	 * @return map may contains following key:
	 * ETLCmd.RESULT_KEY_LOG: list of String user defined log info
	 * ETLCmd.RESULT_KEY_OUTPUT: list of String output
	 * ETLCmd.RESULT_KEY_OUTPUT_MAP: list of Tuple2<key, value>
	 * in the value map, if it contains only 1 value, the key should be ETLCmd.RESULT_KEY_OUTPUT
	 */
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		logger.error("empty map impl, should not be invoked.");
		return null;
	}
	
	/**
	 * reduce function in map-reduce mode
	 * @return list of newKey, newValue, baseOutputPath
	 */
	public List<String[]> reduceProcess(Text key, Iterable<Text> values){
		logger.error("empty reduce impl, should not be invoked.");
		return null;
	}
	
	/**
	 * single thread process
	 * @return list of String user defined log info
	 */
	public List<String> sgProcess(){
		logger.error("empty single impl, should not be invoked.");
		return null;
	}
	
	//for long running command
	public void close(){
	}
	
	///

	public Configuration getHadoopConf(){
		return conf;
	}
	
	public ProcessMode getPm() {
		return pm;
	}

	public void setPm(ProcessMode pm) {
		this.pm = pm;
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
}
