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
	public static final Logger logger = Logger.getLogger(SendLogCmd.class);
	
	public static final String cfgkey_topic = "kafka.log.topic";
	public static final String cfgkey_bootstrap_servers = "kafka.bootstrap.servers";
	private String bootstrapServers;
	private String logTopicName;
	
	public SendLogCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		logTopicName = pc.getString(cfgkey_topic);
		bootstrapServers = pc.getString(cfgkey_bootstrap_servers);
		logger.info(String.format("engine configure: bootstrap:%s, topic:%s", EngineUtil.getInstance().getBootstrapServers(), 
				EngineUtil.getInstance().getLogTopicName()));
		logger.info(String.format("cmd configure: bootstrap:%s, topic:%s", bootstrapServers, logTopicName));
	}

	@Override
	public List<String> sgProcess() {
		logger.info(String.format("use engine config: %b", EngineUtil.getInstance().isSendLog()));
		List<String> loginfo = Arrays.asList(this.otherArgs);
		if (EngineUtil.getInstance().isSendLog()){
			return loginfo;
		}else{
			ETLLog etllog = new ETLLog();
			Producer<String, String> producer = EngineUtil.createProducer(bootstrapServers);
			Date curTime = new Date();
			etllog.setStart(curTime);
			etllog.setEnd(curTime);
			if (wfid!=null) {
				etllog.setWfid(wfid);
			}
			etllog.setActionName(getClass().getName());
			etllog.setCounts(loginfo);
			EngineUtil.sendLog(producer, logTopicName, etllog);
			producer.close();
			return null;
		}
	}
}
