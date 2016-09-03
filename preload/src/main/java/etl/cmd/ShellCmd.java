package etl.cmd;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.StringUtil;

public class ShellCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(ShellCmd.class);
	
	public static final String cfgkey_param_key="key"; //the special key name for mapreduce mode, each key is a line of input
	public static final String cfgkey_command="command";
	
	
	private String command;
	private Map<String, Object> params;

	public ShellCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		command = pc.getString(cfgkey_command);
		params = new HashMap<String, Object>();
		Iterator<String> keys = pc.getKeys();
		while (keys.hasNext()){
			String key = keys.next();
			params.put(key, pc.getProperty(key));
		}
	}

	@Override
	public List<String> sgProcess() {
		try {
			String cmd = StringUtil.fillParams(command, params, "$", "");
			//pass wfid as the last parameter to the shell script
			cmd += " " + this.wfid;
			logger.info(String.format("mr command is %s", cmd));
			CommandLine cmdLine = CommandLine.parse(cmd);
			DefaultExecutor executor = new DefaultExecutor();
			int exitValue = executor.execute(cmdLine);
			logger.info(String.format("process for key:%s ended with exitValue %d.", params.get(cfgkey_param_key), exitValue));
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		params.put(cfgkey_param_key, row);//param passed
		sgProcess();
		return null;
	}
}
