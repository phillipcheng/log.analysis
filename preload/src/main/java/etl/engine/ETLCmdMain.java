package etl.engine;

import static java.util.concurrent.TimeUnit.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ETLCmdMain {
	
	public static final Logger logger = LogManager.getLogger(ETLCmdMain.class);
	public static final String UNUSED = "unused";
	public static final int mandatoryArgNum=4;
	
	public static final String param_exe_interval="exe.interval";
	public static final String param_exe_time="exe.time";//number of seconds to let the task run
	
	
	public static String usage(){
		return "ETLCmdMain: CmdClassName wfName wfid staticConfigFile [defaultFs] ...(other arguments)";
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
			String wfName = args[1];
			String wfid = args[2];
			String strStaticCfgs = args[3];
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
			final ETLCmd[] cmds = EngineUtil.getInstance().getCmds(strCmdClassNames, strStaticCfgs, wfName, wfid, defaultFs, 
					otherArgs, ProcessMode.SingleProcess);
			if (cmds!=null){
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
}
