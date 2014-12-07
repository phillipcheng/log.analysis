package log.analysis.driver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.cld.util.StringUtil;

public class InvokeReducer extends Reducer<Text,LongWritable,Void,Void> {
	
	public static final Logger logger = Logger.getLogger(InvokeReducer.class);
	public static final String PARAM_KEY="key";
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context ) throws IOException, InterruptedException {
		logger.info(String.format("in reducer, key:%s, values:%s", key, values));
		//start the script configured passing the parameters
		try{
			MRPreloadConfig mrpc = new MRPreloadConfig("mrpreload.properties");
			String command = mrpc.getMrCommand();
			Map<String, Object> params = new HashMap<String, Object>();
			params.put(PARAM_KEY, key.toString());
			command = StringUtil.fillParams(command, params, "$", "");
			logger.info(String.format("mr command is %s", command));
			CommandLine cmdLine = CommandLine.parse(command);
			DefaultExecutor executor = new DefaultExecutor();
			int exitValue = executor.execute(cmdLine);
			logger.info(String.format("process for key:%s ended with exitValue %d.", key, exitValue));
		}catch(Exception e){
			logger.error("", e);
		}
	}
}
	