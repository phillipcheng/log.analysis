package etl.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import etl.engine.types.InputFormatType;
import etl.engine.types.OutputFormat;
import etl.engine.types.ProcessMode;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;

public class SaveDataCmd extends SchemaETLCmd {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LogManager.getLogger(SaveDataCmd.class);
	
	public static final @ConfigKey String cfgkey_log_tmp_dir="log.tmp.dir";
	
	private String logTmpDir;
	private etl.engine.types.OutputFormat outputFormat = etl.engine.types.OutputFormat.text;
	
	public SaveDataCmd(){
		super();
	}
	
	public SaveDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public SaveDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		String tmpDirExp = this.getPc().getString(cfgkey_log_tmp_dir);
		logger.info(String.format("%s_exp:%s", cfgkey_log_tmp_dir, tmpDirExp));
		if (tmpDirExp!=null){
			CompiledScript cs = ScriptEngineUtil.compileScript(tmpDirExp);
			logTmpDir = ScriptEngineUtil.eval(cs, super.getSystemVariables());
			logger.info(String.format("%s:%s", cfgkey_log_tmp_dir, logTmpDir));
		}
		outputFormat = etl.engine.types.OutputFormat.valueOf(super.getCfgString(cfgkey_output_file_format, etl.engine.types.OutputFormat.text.toString()));
	}
	
	public boolean hasReduce(){
		return false;
	}
	
	//do not really save file, generate the list of (key->file names)
	public Map<String, Object> mapProcess(long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception {
		if (super.inputFormatType==InputFormatType.CombineFileName || inputFormatType==InputFormatType.FileName){
			//each row is the file name
			context.write(new Text(row), null);
		}else{
			if (offset==0){
				String key = getTableNameSetFileNameByContext(context);
				String value = (String) getSystemVariables().get(VAR_NAME_PATH_NAME);
				context.write(new Text(key), new Text(value));
			}
		}
		return null;
	}
	
	@Override
	public boolean useSparkSql(){
		return true;
	}
	
	@Override
	public JavaPairRDD<String,String> dataSetProcess(JavaSparkContext jsc, SparkSession spark, Map<String, Dataset<Row>> dfMap){
		List<Tuple2<String, String>> l = new ArrayList<Tuple2<String, String>>();
		for (String key:dfMap.keySet()){
			String fileName = String.format("%s%s%s", defaultFs, logTmpDir, key);
			Dataset<Row> ds = dfMap.get(key);
			if (outputFormat == OutputFormat.text){
				ds.write().csv(fileName);
			}else if (outputFormat == OutputFormat.parquet){
				ds.write().parquet(fileName);
			}else{
				logger.error(String.format("format:%s not supported.", outputFormat));
			}
			l.add(new Tuple2<String,String>(key, fileName));	
		};
		return jsc.parallelizePairs(l);
	}
	
	public String getLogTmpDir() {
		return logTmpDir;
	}

	public void setLogTmpDir(String logTmpDir) {
		this.logTmpDir = logTmpDir;
	}
}
