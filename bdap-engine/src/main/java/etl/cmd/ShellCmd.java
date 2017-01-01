package etl.cmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import bdap.util.ParamUtil;
import bdap.util.SystemUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.StringUtil;

public class ShellCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(ShellCmd.class);
	
	//cfgkey
	public static final @ConfigKey String cfgkey_param_key="key"; //the special key name for mapreduce mode, each key is a line of input
	public static final @ConfigKey String cfgkey_command="command";
	
	public static final String capture_prefix="capture:";//from the stdout of the shell script, we filter all the lines started with this
	public static final String key_value_sep=":";//
	
	
	private String command;
	private Map<String, Object> params;

	public ShellCmd(){
		super();
	}
	
	public ShellCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public ShellCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		command = super.getCfgString(cfgkey_command, null);
		if (command==null){
			logger.error(String.format("command can't be null."));
			return;
		}
		params = this.getSystemVariables();
		Iterator<String> keys = super.getCfgKeys();
		while (keys.hasNext()){
			String key = keys.next();
			params.put(key, super.getCfgProperty(key));
		}
	}

	private List<String> processRow(String row) {
		try {
			if (row.contains(ParamUtil.kvSep)){
				Map<String, String> keyValueMap = ParamUtil.parseMapParams(row);
				params.putAll(keyValueMap);
			}else{
				params.put(cfgkey_param_key, row);
			}
			String cmd = StringUtil.fillParams(command, params, "$", "");
			logger.info(String.format("mr command is %s", cmd));
			String ret = SystemUtil.execCmd(cmd);
			String lines[] = ret.split("\\r?\\n");
			logger.info(String.format("process for key:%s ended. \nstdout:\n%s", params.get(cfgkey_param_key), ret));
			List<String> tl = new ArrayList<String>();
			for (String line: lines){
				if (line.startsWith(capture_prefix)){
					tl.add(line.substring(capture_prefix.length()));
				}
			}
			return tl;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) {
		List<String> vl = processRow(row);
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put(RESULT_KEY_OUTPUT_LINE, vl);
		return ret;
	}
	
	@Override
	public JavaRDD<String> sparkProcess(JavaRDD<String> input, JavaSparkContext sc){
		JavaRDD<String> ret = input.flatMap(new FlatMapFunction<String, String>(){
			@Override
			public Iterator<String> call(String t) throws Exception {
				return processRow(t).iterator();
			}
		});
		return ret;
	}

	@Override
	public boolean hasReduce(){
		return false;
	}
}
