package etl.cmd;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.ETLLog;
import etl.engine.EngineUtil;

public class SendLogCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = Logger.getLogger(SendLogCmd.class);
	
	private transient KafkaAdaptorCmd kac;
	
	public SendLogCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		kac = new KafkaAdaptorCmd(pc);
	}

	@Override
	public List<String> sgProcess() {
		logger.info(String.format("use engine config: %b", EngineUtil.getInstance().isSendLog()));
		List<String> loginfo = Arrays.asList(this.otherArgs);
		if (EngineUtil.getInstance().isSendLog()){
			return loginfo;
		}else{
			ETLLog etllog = new ETLLog();
			Producer<String, String> producer = EngineUtil.createProducer(kac.getBootstrapServers());
			Date curTime = new Date();
			etllog.setStart(curTime);
			etllog.setEnd(curTime);
			if (wfid!=null) {
				etllog.setWfid(wfid);
			}
			etllog.setActionName(getClass().getName());
			etllog.setCounts(loginfo);
			EngineUtil.sendLog(producer, kac.getLogTopicName(), etllog);
			producer.close();
			return null;
		}
	}
}
