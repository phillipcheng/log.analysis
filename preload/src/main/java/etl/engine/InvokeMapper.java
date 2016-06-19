package etl.engine;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class InvokeMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	public static final Logger logger = Logger.getLogger(InvokeMapper.class);
	
	public static final String cfgkey_cmdclassname = "cmdClassName";
	public static final String cfgkey_wfid = "wfid";
	public static final String cfgkey_staticconfigfile = "staticConfigFile";
	
	private ETLCmd cmd = null;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		if (cmd == null){
			String inputdir = context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir");
			String cmdClassName = context.getConfiguration().get(cfgkey_cmdclassname);
			String wfid = context.getConfiguration().get(cfgkey_wfid);
			String staticConfigFile = context.getConfiguration().get(cfgkey_staticconfigfile);
			logger.info(String.format("input file:%s, cmdClassName:%s, wfid:%s, staticConfigFile:%s", inputdir, cmdClassName, wfid, staticConfigFile));
			try{
				cmd = (ETLCmd) Class.forName(cmdClassName).getConstructor(String.class, String.class, String.class, String.class, String.class).
						newInstance(wfid, staticConfigFile, null, null, context.getConfiguration().get("fs.defaultFS"));
			}catch(Throwable e){
				logger.error("", e);
			}
		}
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
    
	/**
	 * following parameters should be in the config
	 * CmdClassName:
	 * wfid:
	 * staticConfigFile:
	 */
	
	//for each line of the inputfile, this will be invoked once
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		logger.info(String.format("in mapper, key:%s, values:%s", key, value));
		try{
			List<String> output = cmd.process(key.get(), value.toString(), context);
			if (output!=null){
				for (String line:output){
					context.write(new Text(line), NullWritable.get());
				}
			}
		}catch(Throwable e){
			logger.error("", e);
		}
	}
}