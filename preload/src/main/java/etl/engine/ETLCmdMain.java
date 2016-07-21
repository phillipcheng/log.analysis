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
			String strCmdClassNames = args[0];
			String[] cmdClassNames = strCmdClassNames.split(",");
			String wfid = args[1];
			String strStaticCfgs = args[2];
			String[] staticCfgs = strStaticCfgs.split(",");
			if (cmdClassNames.length!=staticCfgs.length){
				logger.error(String.format("multiple cmds with different cmdClass size and staticCfg size. %d, %d", cmdClassNames.length, staticCfgs.length));
				return;
			}
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
			ETLCmd[] cmds = new ETLCmd[cmdClassNames.length];
			for (int i=0; i<cmdClassNames.length; i++){
				String cmdClassName = cmdClassNames[i];
				String staticCfg = staticCfgs[i];
				try {
					Class clazz = Class.forName(cmdClassName);
					ETLCmd cmd = (ETLCmd) clazz.getConstructor(String.class, String.class, String.class, String.class, String.class).
							newInstance(wfid, staticCfg, dynInCfg, dynOutCfg, defaultFs);
					cmd.setPm(ProcessMode.SingleProcess);
					cmds[i] = cmd;
				}catch(Exception e){
					logger.error("", e);
				}
			}
			String input = null;
			try{
				EngineUtil.getInstance().processMapperCmds(cmds, 0, input, null);
			}catch(Throwable e){
				logger.error("", e);
			}
		}
	}
}
