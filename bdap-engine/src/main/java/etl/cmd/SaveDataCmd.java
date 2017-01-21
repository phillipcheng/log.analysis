package etl.cmd;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import etl.engine.InputFormatType;
import etl.engine.ProcessMode;
import etl.output.ParquetOutputFormat;
import etl.spark.RDDMultipleTextOutputFormat;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;

public class SaveDataCmd extends SchemaETLCmd {
	private static final long serialVersionUID = 1L;
	//cfgkey
	public static final @ConfigKey String cfgkey_log_tmp_dir="log.tmp.dir";
	
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
		logger.info(String.format("%s_exp:%s", cfgkey_log_tmp_dir, tmpDirExp));
		if (tmpDirExp!=null){
			CompiledScript cs = ScriptEngineUtil.compileScript(tmpDirExp);
			logTmpDir = ScriptEngineUtil.eval(cs, super.getSystemVariables());
			logger.info(String.format("%s:%s", cfgkey_log_tmp_dir, logTmpDir));
		}
	}
	
	@Override
	public boolean hasReduce(){
		return false;
	}
	
	//do not really save file, generate the list of (key->file names)
	@Override
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
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc, 
			Class<? extends InputFormat> inputFormatClass){
		logger.info(String.format("%s:%s", cfgkey_log_tmp_dir, logTmpDir));
		copyConf();
		if (ParquetInputFormat.class.isAssignableFrom(inputFormatClass)) {
			input.saveAsNewAPIHadoopFile(String.format("%s%s", super.getDefaultFs(), logTmpDir), Text.class, Text.class, ParquetOutputFormat.class, getHadoopConf());
		} else
			input.saveAsHadoopFile(String.format("%s%s", super.getDefaultFs(), logTmpDir), Text.class, Text.class, RDDMultipleTextOutputFormat.class);
		//TODO use input.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass);
		return input.groupByKey().keys().flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				init();
				String path = String.format("%s%s%s-*", getDefaultFs(), logTmpDir, t);
				FileStatus[] fstatlist = fs.globStatus(new Path(path));
				List<Tuple2<String,String>> ret = new ArrayList<Tuple2<String,String>>();
				for (FileStatus fstat:fstatlist){
					logger.debug(String.format("save cmd, key:%s, path:%s", t, fstat.getPath().toString()));
					ret.add(new Tuple2<String, String>(t, fstat.getPath().toString()));
				}
				return ret.iterator();
			}
		});
	}
	
	public String getLogTmpDir() {
		return logTmpDir;
	}

	public void setLogTmpDir(String logTmpDir) {
		this.logTmpDir = logTmpDir;
	}
}
