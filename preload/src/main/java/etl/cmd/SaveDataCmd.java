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
	
	public SaveDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, defaultFs, otherArgs);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, defaultFs, otherArgs);
		logTmpDir = this.getPc().getString(cfg_log_tmp_dir);
	}
	
	public JavaRDD<Tuple2<String, String>> sparkProcessKeyValue(JavaRDD<Tuple2<String, String>> input, JavaSparkContext jsc){
		SparkUtil.saveByKey(input, defaultFs, logTmpDir, wfid);
		return null;
	}
}
