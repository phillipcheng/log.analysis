package etl.cmd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import etl.engine.ETLCmd;
import etl.spark.SparkUtil;
import scala.Tuple2;

public class SaveDataCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;
	
	public static final String cfg_log_tmp_dir="log.tmp.dir";
	
	private String logTmpDir;
	
	public String getLogTmpDir() {
		return logTmpDir;
	}

	public void setLogTmpDir(String logTmpDir) {
		this.logTmpDir = logTmpDir;
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
	public JavaRDD<Tuple2<String, String>> sparkProcessKeyValue(JavaRDD<Tuple2<String, String>> input){
		SparkUtil.saveByKey(input, defaultFs, logTmpDir, wfid);
		return null;
	}
}
