package etl.cmd;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.configuration.PropertiesConfiguration;

import etl.engine.EngineUtil;

public class KafkaAdaptorCmd{

	public static final Logger logger = LogManager.getLogger(KafkaAdaptorCmd.class);
	
	//
	public static final String cfgkey_topic = "kafka.log.topic";
	public static final String cfgkey_bootstrap_servers = "kafka.bootstrap.servers";
	
	private String bootstrapServers;
	private String logTopicName;
	
	public KafkaAdaptorCmd(PropertiesConfiguration pc){
		logTopicName = pc.getString(cfgkey_topic);
		bootstrapServers = pc.getString(cfgkey_bootstrap_servers);
		logger.info(String.format("engine configure: bootstrap:%s, topic:%s", EngineUtil.getInstance().getBootstrapServers(), 
				EngineUtil.getInstance().getLogTopicName()));
		logger.info(String.format("cmd configure: bootstrap:%s, topic:%s", bootstrapServers, logTopicName));
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getLogTopicName() {
		return logTopicName;
	}

	public void setLogTopicName(String logTopicName) {
		this.logTopicName = logTopicName;
	}
	
	
}
