package etl.engine;

import static java.util.concurrent.TimeUnit.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.log4j.Logger;

public class ETLCmdMain {
	
	public static final Logger logger = Logger.getLogger(ETLCmdMain.class);
	public static final String UNUSED = "unused";
	public static final int mandatoryArgNum=3;
	
	public static final String param_exe_interval="exe.interval";
	public static final String param_exe_time="exe.time";//number of seconds to let the task run
	
	
	public static String usage(){
		return "ETLCmdMain: CmdClassName wfid staticConfigFile [defaultFs] ...(other arguments)";
	}
	
	//
	private static Map<String, String> extractValues(String[] otherArgs){
		Map<String, String> kvMap = new HashMap<String, String>();
		if (otherArgs!=null){
			for (String arg: otherArgs){
				String[] kv = arg.split("=", 2);
				if (kv.length==2){
					kvMap.put(kv[0], kv[1]);
				}
			}
		}
		return kvMap;
	}
	
	//this is the java action
	public static void main(String[] args){
		if (args.length<mandatoryArgNum){
			logger.error(usage());
		}else{
			String strCmdClassNames = args[0];
			String[] cmdClassNames = strCmdClassNames.split(",", -1);
			String wfid = args[1];
			String strStaticCfgs = args[2];
			String[] staticCfgs = strStaticCfgs.split(",", -1);
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
			final ETLCmd[] cmds = new ETLCmd[cmdClassNames.length];
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
			final String input = null;
			int exeInterval=0;
			int exeSeconds=0;
			if (otherArgs!=null){
				Map<String, String> kvMap = extractValues(otherArgs);
				String v = kvMap.get(param_exe_interval);
				if (v!=null){
					exeInterval = Integer.parseInt(v);
				}
				v = kvMap.get(param_exe_time);
				if (v!=null){
					exeSeconds = Integer.parseInt(v);
				}
			}
			if (exeInterval==0){//one shot
				try{
					EngineUtil.getInstance().processMapperCmds(cmds, 0, input, null);
				}catch(Throwable e){
					logger.error("", e);
				}
			}else{//repeated
				ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
				final ScheduledFuture<?> taskHandler = scheduler.scheduleAtFixedRate(
						new Runnable(){
							@Override
							public void run() {
								try{
									EngineUtil.getInstance().processMapperCmds(cmds, 0, input, null);
								}catch(Throwable e){
									logger.error("", e);
								}
							}}, 
						5, exeInterval, SECONDS);
				if (exeSeconds>0){
					scheduler.schedule(new Runnable(){
						@Override
						public void run() {
							taskHandler.cancel(true);
							for (ETLCmd cmd:cmds){
								cmd.close();
							}
						}}, exeSeconds, SECONDS);
				}
				try {
					taskHandler.get();
				}catch(CancellationException ce){
					logger.info("cancelled.");
				}catch(Exception e){
					logger.error("", e);
				}
				scheduler.shutdown();
			}
		}
	}
}
