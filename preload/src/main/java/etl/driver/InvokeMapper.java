package etl.driver;

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
	public static final String PARAM_KEY="key";
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		logger.info(String.format("in mapper, key:%s, values:%s", key, value));
		//start the script configured passing the parameters
		try{
			String inputdir = context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir");
			logger.info(String.format("input file:%s", inputdir));
			MRPreloadConfig mrpc = new MRPreloadConfig("mrpreload.properties");
			String command = mrpc.getMrCommand();
			Map<String, Object> params = new HashMap<String, Object>();
			params.put(PARAM_KEY, value.toString());//value is file name
			command = StringUtil.fillParams(command, params, "$", "");
			logger.info(String.format("mr command is %s", command));
			CommandLine cmdLine = CommandLine.parse(command);
			DefaultExecutor executor = new DefaultExecutor();
			int exitValue = executor.execute(cmdLine);
			logger.info(String.format("process for key:%s ended with exitValue %d.", value, exitValue));
		}catch(Exception e){
			logger.error("", e);
		}
	}
}