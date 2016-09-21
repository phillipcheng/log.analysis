package etl.spark;

import java.util.Date;
import java.util.List;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import etl.engine.ETLCmd;
import etl.engine.EngineUtil;
import etl.engine.ProcessMode;

public class CmdReciever extends Receiver<String> {
	public static final String WFID_SEP=",";
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LogManager.getLogger(CmdReciever.class);
	
	ETLCmd cmd;
	private int sleepSeconds;
	
	public CmdReciever(StorageLevel storageLevel) {
		super(storageLevel);
	}
	
	public CmdReciever(StorageLevel storageLevel, String cmdClassName, String wfName, 
			String cfgProperties, String defaultFs, int sleepSeconds) {
		super(storageLevel);
		this.sleepSeconds = sleepSeconds;
		cmd = EngineUtil.getInstance().getCmd(cmdClassName, cfgProperties, wfName, null, 
				defaultFs, null, ProcessMode.SingleProcess);
	}

	@Override
	public void onStart() {
		new Thread(){
			@Override
			public void run(){
				try {
					while (true){
						String wfId = String.valueOf((new Date()).getTime());
						cmd.setWfid(wfId);
						cmd.reinit();
						SparkReciever reciever = (SparkReciever)cmd;
						List<String> files = reciever.sparkRecieve();
						for (String file:files){
							store(wfId + WFID_SEP + file);
						}
						Thread.sleep(sleepSeconds*1000);
					}
				}catch(Throwable t){
					logger.error("", t);
				}
			}
		}.start();
	}

	@Override
	public void onStop() {
		
	}

}
