package etl.cmd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import etl.engine.ETLCmd;
import etl.spark.SparkUtil;

public class SaveDataCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;
	//cfgkey
	public static final String cfg_log_tmp_dir="log.tmp.dir";
	
	private String logTmpDir;

	public SaveDataCmd(){
		super();
	}
	
	public SaveDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		logTmpDir = this.getPc().getString(cfg_log_tmp_dir);
	}
	
	@Override
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc){
		SparkUtil.saveByKey(input, defaultFs, logTmpDir, wfid);
		return null;
	}
	
	public String getLogTmpDir() {
		return logTmpDir;
	}

	public void setLogTmpDir(String logTmpDir) {
		this.logTmpDir = logTmpDir;
	}
}
