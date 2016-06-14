package etl.cmd;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.util.StringUtil;

public class ShellCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(ShellCmd.class);
	public static final String PARAM_KEY="key";
	public static final String PROP_CMD="command";

	public ShellCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
	}

	@Override
	public List<String> process(String param) {
		try {
			String command = pc.getString(PROP_CMD);
			Map<String, Object> params = new HashMap<String, Object>();
			Iterator<String> keys = pc.getKeys();
			while (keys.hasNext()){
				String key = keys.next();
				params.put(key, pc.getProperty(key));
			}
			params.put(PARAM_KEY, param);//value is file name
			command = StringUtil.fillParams(command, params, "$", "");
			//pass wfid as the last parameter to the shell script
			command += " " + this.wfid;
			logger.info(String.format("mr command is %s", command));
			CommandLine cmdLine = CommandLine.parse(command);
			DefaultExecutor executor = new DefaultExecutor();
			int exitValue = executor.execute(cmdLine);
			logger.info(String.format("process for key:%s ended with exitValue %d.", param, exitValue));
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
}
