package etl.cmd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;

public class CsvFilterCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(CsvFilterCmd.class);
	
	//cfgkey
	public static final String cfgkey_filter_exp="filter.exp";
	
	private transient CompiledScript filterExp;

	public CsvFilterCmd(){
		super();
	}
	
	public CsvFilterCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public CsvFilterCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		String filterExpStr = super.getCfgString(cfgkey_filter_exp, null);
		if (filterExpStr!=null){
			filterExp = ScriptEngineUtil.compileScript(filterExpStr);
		}else{
			logger.error(String.format("%s can't be null.", cfgkey_filter_exp));
		}
	}

	private Tuple2<String, String> processRow(String row) {
		super.init();
		String[] fields = row.split(",");
		super.getSystemVariables().put(ETLCmd.VAR_FIELDS, fields);
		if (filterExp!=null){
			String key = ScriptEngineUtil.eval(filterExp, super.getSystemVariables());
			return new Tuple2<String, String>(key, row);
		}else{
			return null;
		}
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		Tuple2<String, String> v = processRow(row);
		context.write(new Text(v._1), new Text(row));
		return null;
	}
	
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		for (Text v: values){
			mos.write(v, null, key.toString());
		}
		return null;
	}
	
	@Override
	public Map<String, JavaRDD<String>> sparkSplitProcess(JavaRDD<String> input, JavaSparkContext jsc){
		Map<String, JavaRDD<String>> retMap = new HashMap<String, JavaRDD<String>>();
		JavaPairRDD<String, String> kv = input.mapToPair(new PairFunction<String, String, String>(){
			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				return processRow(t);
			}
		});
		List<String> ks = kv.keys().collect();
		for (String k: ks){
			JavaPairRDD<String, String> fv = kv.filter(new Function<Tuple2<String, String>, Boolean>(){
				@Override
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					if (k.equals(v1._1)){
						return true;
					}else{
						return false;
					}
				}
			});
			retMap.put(k, fv.values());
		}
		return retMap;
	}
}
