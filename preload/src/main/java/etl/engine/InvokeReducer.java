package etl.engine;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;


public class InvokeReducer extends Reducer<Text, Text, Text, Text>{
	public static final Logger logger = Logger.getLogger(InvokeMapper.class);
	
	public static final String cfgkey_cmdclassname = "cmdClassName";
	public static final String cfgkey_wfid = "wfid";
	public static final String cfgkey_staticconfigfile = "staticConfigFile";
	
	private ETLCmd[] cmds = null;
	
	private MultipleOutputs<Text, Text> mos;
	
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
		mos = new MultipleOutputs<Text,Text>(context);
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	mos.close();
    }
    
	/**
	 * following parameters should be in the config
	 * CmdClassName:
	 * wfid:
	 * staticConfigFile:
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ETLCmd cmd = cmds[0];
		String[] ret = cmd.reduceProcess(key, values);
		if (ETLCmd.SINGLE_TABLE.equals(ret[2])){
			context.write(new Text(ret[0]), new Text(ret[1]));
		}else{
			mos.write(new Text(ret[0]), new Text(ret[1]), ret[2]);
		}
	}
}
