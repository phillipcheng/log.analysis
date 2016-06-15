package etl.engine;

import org.apache.log4j.Logger;

public class ETLCmdMain {
	public static final Logger logger = Logger.getLogger(ETLCmdMain.class);
	public static final String UNUSED = "unused";
	public static String usage(){
		return "ETLCmdMain: CmdClassName wfid staticConfigFile dynInConfigFile dynOutConfigFile [defaultFs]";
	}
	
	//this is the java action
	public static void main(String[] args){
		if (args.length<5){
			logger.error(usage());
		}else{
			String cmdClassName = args[0];
			String wfid = args[1];
			String staticCfg = args[2];
			String dynInCfg = args[3];
			if (UNUSED.equals(dynInCfg)){
				dynInCfg = null;
			}
			String dynOutCfg = args[4];
			if (UNUSED.equals(dynOutCfg)){
				dynOutCfg = null;
			}
			String defaultFs = null;
			if (args.length>5){
				defaultFs = args[5];
				if (UNUSED.equals(defaultFs)){
					defaultFs = null;
				}
			}
			try {
				Class clazz = Class.forName(cmdClassName);
				ETLCmd cmd = (ETLCmd) clazz.getConstructor(String.class, String.class, String.class, String.class, String.class).
						newInstance(wfid, staticCfg, dynInCfg, dynOutCfg, defaultFs);
				cmd.process(null, null);
			}catch(Exception e){
				logger.error("", e);
			}
		}
	}
}
