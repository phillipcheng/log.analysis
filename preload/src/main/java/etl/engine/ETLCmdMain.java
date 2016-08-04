package etl.engine;

import java.util.Arrays;

import org.apache.log4j.Logger;

public class ETLCmdMain {
	
	public static final Logger logger = Logger.getLogger(ETLCmdMain.class);
	public static final String UNUSED = "unused";
	public static final int mandatoryArgNum=3;
	
	public static String usage(){
		return "ETLCmdMain: CmdClassName wfid staticConfigFile [defaultFs] ...(other arguments)";
	}
	
	//this is the java action
	public static void main(String[] args){
		if (args.length<mandatoryArgNum){
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
			String defaultFs = null;
			if (args.length>mandatoryArgNum){//optional defaultFs
				defaultFs = args[mandatoryArgNum];
				if (UNUSED.equals(defaultFs)){
					defaultFs = null;
				}
			}
			String[] otherArgs = null;
			if (args.length>mandatoryArgNum+1){//otherArgs
				otherArgs = Arrays.copyOfRange(args, mandatoryArgNum+1, args.length);
			}
			ETLCmd[] cmds = new ETLCmd[cmdClassNames.length];
			for (int i=0; i<cmdClassNames.length; i++){
				String cmdClassName = cmdClassNames[i];
				String staticCfg = staticCfgs[i];
				try {
					Class clazz = Class.forName(cmdClassName);
					ETLCmd cmd = (ETLCmd) clazz.getConstructor(String.class, String.class, String.class, String[].class).
							newInstance(wfid, staticCfg, defaultFs, otherArgs);
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
