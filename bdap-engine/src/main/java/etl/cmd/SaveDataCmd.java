package etl.cmd;

import java.util.List;

import javax.script.CompiledScript;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.spark.RDDMultipleTextOutputFormat;
import etl.spark.SparkUtil;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;

public class SaveDataCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;
	//cfgkey
	public static final String cfgkey_log_tmp_dir="log.tmp.dir";
	
	private String logTmpDir;

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
		String tmpDirExp = this.getPc().getString(cfgkey_log_tmp_dir);
		CompiledScript cs = ScriptEngineUtil.compileScript(tmpDirExp);
		logTmpDir = ScriptEngineUtil.eval(cs, super.getSystemVariables());
		
	}
	
	@Override
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc){
		input.saveAsHadoopFile(String.format("%s%s", super.getDefaultFs(), logTmpDir), Text.class, Text.class, RDDMultipleTextOutputFormat.class);
		return null;
	}
	
	public String getLogTmpDir() {
		return logTmpDir;
	}

	public void setLogTmpDir(String logTmpDir) {
		this.logTmpDir = logTmpDir;
	}
}
