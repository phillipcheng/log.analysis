package etl.engine;

import java.io.IOException;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.EngineConf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvokeMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static final Logger logger = LogManager.getLogger(InvokeMapper.class);
	
	private ETLCmd[] cmds = null;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		if (cmds == null){
			String inputdir = context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir");
			String wfid = context.getConfiguration().get(EngineConf.cfgkey_wfid);
			String wfName = context.getConfiguration().get(EngineConf.cfgkey_wfName);
			String strCmdClassNames = context.getConfiguration().get(EngineConf.cfgkey_cmdclassname);
			String strStaticConfigFiles = context.getConfiguration().get(EngineConf.cfgkey_staticconfigfile);
			String defaultFs = context.getConfiguration().get("fs.defaultFS");
			logger.info(String.format("input file:%s, cmdClassName:%s, wfid:%s, staticConfigFile:%s, %s", inputdir, strCmdClassNames, wfid, 
					strStaticConfigFiles, defaultFs));
			cmds = EngineUtil.getInstance().getCmds(strCmdClassNames, strStaticConfigFiles, wfName, wfid, defaultFs, null, ProcessMode.Map);
		}
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	if (cmds != null)
    		for (ETLCmd c: cmds)
    			c.close();
    }
    
	//for each line of the inputfile, this will be invoked once
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (cmds!=null){
			logger.debug(String.format("in mapper, key:%s, values:%s", key, value));
			String input = value.toString();
			EngineUtil.getInstance().processMapperCmds(cmds, key.get(), input, context);
		}
	}
}