package etl.cmd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import etl.engine.ETLCmd;
import etl.util.StringUtil;
import etl.util.Util;

public class ShellCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(ShellCmd.class);
	
	//cfgkey
	public static final String cfgkey_param_key="key"; //the special key name for mapreduce mode, each key is a line of input
	public static final String cfgkey_command="command";
	
	
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

	@Override
	public List<String> sgProcess() {
		try {
			String cmd = StringUtil.fillParams(command, params, "$", "");
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
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) {
		if (row.contains(Util.kvSep)){
			Map<String, String> keyValueMap = Util.parseMapParams(row);
			params.putAll(keyValueMap);
		}else{
			params.put(cfgkey_param_key, row);
		}
		sgProcess();
		return null;
	}
	

	@Override
	public boolean hasReduce(){
		return false;
	}
}
