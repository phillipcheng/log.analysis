package etl.engine;

import org.apache.log4j.Logger;

public class ETLCmdMain {
	public static final Logger logger = Logger.getLogger(ETLCmdMain.class);
	
	public static String usage(){
		return "CmdClassName defaultFs wfid staticConfigFile dynamicConfigFile";
	}
	
	//this is the java action
	public static void main(String[] args){
		if (args.length<5){
			logger.error(usage());
		}else{
			String cmdClassName = args[0];
			String defaultFs = args[1];
			String wfid = args[2];
			String staticCfg = args[3];
			String dynCfg = args[4];
			try {
				Class clazz = Class.forName(cmdClassName);
				ETLCmd cmd = (ETLCmd) clazz.getConstructor(String.class, String.class, String.class, String.class).
						newInstance(defaultFs, wfid, staticCfg, dynCfg);
				cmd.process();
			}catch(Exception e){
				logger.error("", e);
			}
		}
	}
}
