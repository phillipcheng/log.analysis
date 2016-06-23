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
	
	private ETLCmd[] cmds = null;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		if (cmds == null){
			String inputdir = context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir");
			String strCmdClassNames = context.getConfiguration().get(cfgkey_cmdclassname);
			String[] cmdClassNames = strCmdClassNames.split(",");
			String wfid = context.getConfiguration().get(cfgkey_wfid);
			String strStaticConfigFiles = context.getConfiguration().get(cfgkey_staticconfigfile);
			String[] staticCfgFiles = strStaticConfigFiles.split(",");
			
			logger.info(String.format("input file:%s, cmdClassName:%s, wfid:%s, staticConfigFile:%s", inputdir, strCmdClassNames, wfid, strStaticConfigFiles));
			try{
				cmds = new ETLCmd[cmdClassNames.length];
				for (int i=0; i<cmds.length; i++){
					cmds[i] = (ETLCmd) Class.forName(cmdClassNames[i]).getConstructor(String.class, String.class, String.class, String.class, String.class).
							newInstance(wfid, staticCfgFiles[i], null, null, context.getConfiguration().get("fs.defaultFS"));
				}
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
			String input = value.toString();
			for (int i=0; i<cmds.length; i++){
				ETLCmd cmd = cmds[i];
				List<String> outputs = cmd.process(key.get(), input, context);
				if (i<cmds.length-1){//intermediate steps
					if (outputs!=null && outputs.size()==1){
						input = outputs.get(0);
					}else{
						String outputString = "null";
						if (outputs!=null){
							outputString = outputs.toString();
						}
						logger.error(String.format("output from chained cmd should be a string. %s", outputString));
					}
				}else{//last step
					if (outputs!=null){
						for (String line:outputs){
							context.write(new Text(line), NullWritable.get());
						}
					}
				}
			}
		}catch(Throwable e){
			logger.error("", e);
		}
	}
}