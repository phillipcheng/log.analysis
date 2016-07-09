package etl.engine;

import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.util.Util;

public abstract class ETLCmd {
	public static final Logger logger = Logger.getLogger(ETLCmd.class);
	
	public static final String RESULT_KEY_LOG="log";
	public static final String RESULT_KEY_OUTPUT="output";
	
	protected String wfid;
	protected FileSystem fs;
	protected Map<String, List<String>> dynCfgMap;
	protected PropertiesConfiguration pc;
	protected String outDynCfg;
	private Configuration conf;
	
	private ProcessMode pm = ProcessMode.SingleProcess;
	private MRMode mrMode = MRMode.file;
	
	public ETLCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		this.wfid = wfid;
		try {
			conf = new Configuration();
			if (defaultFs!=null){
				conf.set("fs.defaultFS", defaultFs);
			}
			this.fs = FileSystem.get(conf);
		}catch(Exception e){
			logger.error("", e);
		}
		if (inDynCfg!=null){
			dynCfgMap = (Map<String, List<String>>) Util.fromDfsJsonFile(fs, inDynCfg, Map.class);
		}
		this.pc = Util.getPropertiesConfigFromDfs(fs, staticCfg);
		this.outDynCfg = outDynCfg;
	}
	
	/**
	 * @return map may contains following key:
	 * RESULT_KEY_LOG: list of String user defined log info
	 * RESULT_KEY_OUTPUT: list of String output
	 */
	public Map<String, List<String>> mrProcess(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context){
		logger.error("empty impl, should not be invoked.");
		return null;
	}
	
	/**
	 * @return list of String user defined log info
	 */
	public List<String> sgProcess(){
		logger.error("empty impl, should not be invoked.");
		return null;
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
}
