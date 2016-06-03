package etl.engine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.util.StringUtil;

public class InvokeMapper extends Mapper<Object, Text, Text, LongWritable>{
	public static final Logger logger = Logger.getLogger(InvokeMapper.class);
	
	public static final String cfgkey_cmdclassname = "cmdClassName";
	public static final String cfgkey_wfid = "wfid";
	public static final String cfgkey_staticconfigfile = "staticConfigFile";
	
	/**
	 * following parameters should be in the config
	 * CmdClassName:
	 * wfid:
	 * staticConfigFile:
	 */
	
	//for each line of the inputfile, this will be invoked once
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		logger.info(String.format("in mapper, key:%s, values:%s", key, value));
		try{
			String inputdir = context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir");
			String cmdClassName = context.getConfiguration().get(cfgkey_cmdclassname);
			String wfid = context.getConfiguration().get(cfgkey_wfid);
			String staticConfigFile = context.getConfiguration().get(cfgkey_staticconfigfile);
			logger.info(String.format("input file:%s, cmdClassName:%s, wfid:%s, staticConfigFile:%s", inputdir, cmdClassName, wfid, staticConfigFile));
			Class clazz = Class.forName(cmdClassName);
			ETLCmd cmd = (ETLCmd) clazz.getConstructor(String.class, String.class, String.class, String.class, String.class).
					newInstance(wfid, staticConfigFile, null, null, null);
			logger.info(String.format("invoking cmd.process %s", value.toString()));
			cmd.process(value.toString());
		}catch(Throwable e){
			logger.error("", e);
		}
	}
}