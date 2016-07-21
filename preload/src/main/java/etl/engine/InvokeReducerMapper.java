package etl.engine;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class InvokeReducerMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static final Logger logger = Logger.getLogger(InvokeReducerMapper.class);
	
	public static final String cfgkey_cmdclassname = "cmdClassName";
	public static final String cfgkey_wfid = "wfid";
	public static final String cfgkey_staticconfigfile = "staticConfigFile";
	
	private ETLCmd[] cmds = null;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		if (cmds == null){
			String inputdir = context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir");
			String wfid = context.getConfiguration().get(cfgkey_wfid);
			String strCmdClassNames = context.getConfiguration().get(cfgkey_cmdclassname);
			String strStaticConfigFiles = context.getConfiguration().get(cfgkey_staticconfigfile);
			String defaultFs = context.getConfiguration().get("fs.defaultFS");
			logger.info(String.format("input file:%s, cmdClassName:%s, wfid:%s, staticConfigFile:%s, %s", inputdir, strCmdClassNames, wfid, 
					strStaticConfigFiles, defaultFs));
			cmds = EngineUtil.getCmds(strCmdClassNames, strStaticConfigFiles, wfid, defaultFs);
		}
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		logger.debug(String.format("in mapper, key:%s, values:%s", key, value));
		String input = value.toString();
		try{
			EngineUtil.getInstance().processReducerMapperCmds(cmds, key.get(), input, context);
		}catch(Throwable e){
			logger.error("", e);
		}
	}
}