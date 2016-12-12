package etl.cmd;

import javax.script.CompiledScript;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.spark.SparkUtil;
import etl.util.ScriptEngineUtil;

public class SaveDataCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;
	//cfgkey
	public static final String cfg_log_tmp_dir="log.tmp.dir";
	public static final String cfg_tmp_dir_exp="log.tmp.dir.exp";
	
	private String logTmpDir;
	private transient CompiledScript csLogTmpDir;

	public SaveDataCmd(){
		super();
	}
	
	public SaveDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public SaveDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		logTmpDir = this.getPc().getString(cfg_log_tmp_dir);
		String logTmpDirExp = this.getPc().getString(cfg_tmp_dir_exp, null);
		if (logTmpDirExp!=null){
			csLogTmpDir = ScriptEngineUtil.compileScript(logTmpDirExp);
			logTmpDir = ScriptEngineUtil.eval(csLogTmpDir, super.getSystemVariables());
		}
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
