package etl.cmd;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.EngineUtil;
import etl.log.ETLLog;
import etl.log.LogType;

public class SendLogCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(SendLogCmd.class);
	
	private transient KafkaAdaptorCmd kac;
	
	public SendLogCmd(){
		super();
	}
	
	public SendLogCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfName, wfid, staticCfg, defaultFs, otherArgs);
		kac = new KafkaAdaptorCmd(super.getPc());
	}

	@Override
	public List<String> sgProcess() {
		logger.info(String.format("use engine config: %b", EngineUtil.getInstance().isSendLog()));
		if (EngineUtil.getInstance().isSendLog()){
			return Arrays.asList(this.otherArgs);
		}else{
			ETLLog etllog = getEtllog();
			EngineUtil.getInstance().sendLog(kac.getLogTopicName(), etllog);
			return null;
		}
	}
	
	public ETLLog getEtllog(){
		List<String> loginfo = Arrays.asList(this.otherArgs);
		ETLLog etllog = new ETLLog(LogType.etlstat);
		Date curTime = new Date();
		etllog.setStart(curTime);
		etllog.setEnd(curTime);
		if (super.getWfName()!=null){
			etllog.setWfName(super.getWfName());
		}
		if (wfid!=null) {
			etllog.setWfid(wfid);
		}
		etllog.setActionName(getClass().getName());
		etllog.setCounts(loginfo);
		return etllog;
	}
}
