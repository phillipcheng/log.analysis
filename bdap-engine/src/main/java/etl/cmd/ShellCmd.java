package etl.cmd;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import etl.engine.ETLCmd;
import etl.util.ParamUtil;
import etl.util.StringUtil;
import scala.Tuple2;

public class ShellCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(ShellCmd.class);
	
	//cfgkey
	public static final String cfgkey_param_key="key"; //the special key name for mapreduce mode, each key is a line of input
	public static final String cfgkey_command="command";
	
	public static final String capture_prefix="capture:";//from the stdout of the shell script, we filter all the lines started with this
	public static final String key_value_sep=":";//
	
	
	private String command;
	private Map<String, Object> params;

	public ShellCmd(){
		super();
	}
	
	public ShellCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
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

	private List<Tuple2<String, String>> processRow(String row) {
		try {
			if (row.contains(ParamUtil.kvSep)){
				Map<String, String> keyValueMap = ParamUtil.parseMapParams(row);
				params.putAll(keyValueMap);
			}else{
				params.put(cfgkey_param_key, row);
			}
			String cmd = StringUtil.fillParams(command, params, "$", "");
			logger.info(String.format("mr command is %s", cmd));
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
			DefaultExecutor executor = new DefaultExecutor();
			executor.setStreamHandler(streamHandler);
			CommandLine cmdLine = CommandLine.parse(cmd);
			int exitValue = executor.execute(cmdLine);
			String ret = outputStream.toString();
			String lines[] = ret.split("\\r?\\n");
			logger.info(String.format("process for key:%s ended with exitValue %d. \nstdout:\n%s", params.get(cfgkey_param_key), exitValue, ret));
			List<Tuple2<String, String>> tl = new ArrayList<Tuple2<String,String>>();
			for (String line: lines){
				if (line.startsWith(capture_prefix)){
					String kv = line.substring(capture_prefix.length());
					String[] vs = kv.split(key_value_sep, 2);//split only once
					if (vs.length==2){
						tl.add(new Tuple2<String,String>(vs[0], vs[1]));
					}else{
						logger.error(String.format("%s should be like key:value", kv));
					}
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
		List<Tuple2<String, String>> vl = processRow(row);
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put(RESULT_KEY_OUTPUT_TUPLE2, vl);
		return ret;
	}
	
	@Override
	public JavaRDD<Tuple2<String, String>> sparkProcess(JavaRDD<String> input){
		JavaRDD<Tuple2<String, String>> ret = input.flatMap(new FlatMapFunction<String, Tuple2<String, String>>(){
			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
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
