package etl.engine;

import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

public class EngineUtil {
	public static final Logger logger = Logger.getLogger(EngineUtil.class);
	
	public static String config_file="etlengine.properties";
	public static final String key_bootstrap_servers="kafka.bootstrap.servers";
	public static final String key_kafka_log_topic="kafka.log.topic";
	public static final String key_enable_kafka="kafka.enabled";
	
	private PropertiesConfiguration engineProp = null;
	
	private Producer<String, String> producer = null;
	private String logTopicName;
	private String bootstrapServers;
	private boolean sendLog=false; //engine level send log flag

	public static void setConfFile(String file){
		config_file = file;
		singleton = new EngineUtil();
	}
	
	public Producer<String, String> getProducer(){
		return producer;
	}
	
	public static Producer<String, String> createProducer(String bootstrapServers){
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("request.required.acks", "1");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer<String, String>(props);
	}
	
	public static Consumer<String, String> createConsumer(String bootstrapServers, String[] topics){
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(topics);
		return consumer;
	}
	
	private EngineUtil(){
		InputStream input = this.getClass().getClassLoader().getResourceAsStream(config_file);
		if (input!=null){
			try {
				engineProp = new PropertiesConfiguration();
				engineProp.load(input);
				logTopicName = engineProp.getString(key_kafka_log_topic);
				sendLog = engineProp.getBoolean(key_enable_kafka, false);
				bootstrapServers = engineProp.getString(key_bootstrap_servers);
				logger.info(String.format("kafka producer bootstrap servers:%s", bootstrapServers));
				if (sendLog){
					producer = createProducer(bootstrapServers);
				}
			}catch(Exception e){
				logger.error("failed to get kafka producer.", e);
			}
		}else{
			logger.error(String.format("engine config %s is null.", config_file));
		}
	}
	
	private static EngineUtil singleton = new EngineUtil();
	
	public static EngineUtil getInstance(){
		return singleton;
	}
	
	public static void sendLog(Producer<String, String> producer, String topicName, ETLLog etllog){
		sendMsg(producer, topicName, etllog.toString());
	}
	
	public static void sendMsg(Producer<String, String> producer, String topicName, String msg){
		if (producer!=null){
			logger.info(String.format("kafka produce msg: %s", msg));
			producer.send(new ProducerRecord<String,String>(topicName, msg), new Callback(){
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e!=null){
						logger.error("exception got while send log", e);
					}
				}
			});
		}
	}
	
	public void sendLog(ETLLog etllog){
		sendLog(this.producer, this.logTopicName, etllog);
	}
	
	public void sendLog(ETLCmd cmd, Date startTime, Date endTime, List<String> loginfo){
		if (cmd.isSendLog()){
			ETLLog etllog = new ETLLog();
			etllog.setStart(startTime);
			etllog.setEnd(endTime);
			if (cmd.getWfid()!=null) {
				etllog.setWfid(cmd.getWfid());
			}
			etllog.setActionName(cmd.getClass().getName());
			etllog.setCounts(loginfo);
			sendLog(this.producer, this.logTopicName, etllog);
		}
	}
	
	public void processMapperCmds(ETLCmd[] cmds, long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		String input = row;
		for (int i=0; i<cmds.length; i++){
			ETLCmd cmd = cmds[i];
			if (cmd.getPm()==ProcessMode.MRProcess){
				Date startTime = new Date();
				Map<String, Object> alloutputs = cmd.mapProcess(offset, input, context);
				Date endTime = new Date();
				if (alloutputs!=null){
					if (cmd.getMrMode()==MRMode.file){
						//generate log for file mode mr processing
						List<String> logoutputs = (List<String>) alloutputs.get(ETLCmd.RESULT_KEY_LOG);
						sendLog(cmd, startTime, endTime, logoutputs);
					}
					if (alloutputs.containsKey(ETLCmd.RESULT_KEY_OUTPUT)){
						//for all mapper only cmd, the result should contains the RESULT_KEY_OUTPUT
						List<String> outputs = (List<String>) alloutputs.get(ETLCmd.RESULT_KEY_OUTPUT);
						if (i<cmds.length-1){//intermediate steps
							if (outputs!=null && outputs.size()==1){
								input = outputs.get(0);
							}else{
								String outputString = "null";
								if (outputs!=null){
									outputString = outputs.toString();
								}
								logger.error(String.format("output from chained cmd should be a string. %s", outputString));
							}
						}else{//last step
							if (outputs!=null){
								if (context!=null){
									for (String line:outputs){
										context.write(new Text(line), null);
									}
								}else{
									logger.debug(String.format("final output:%s", outputs));
								}
							}
						}
					}else{//for map-reduce mapper phrase, the result is key-value pair
						if (alloutputs!=null && context!=null){
							for (String key:alloutputs.keySet()){
								context.write(new Text(key), new Text((String) alloutputs.get(key)));
							}
						}
					}
				}
			}else{
				Date startTime = new Date();
				List<String> logoutputs = cmd.sgProcess();
				Date endTime = new Date();
				sendLog(cmd, startTime, endTime, logoutputs);
			}
		}
	}
	
	public static ETLCmd[] getCmds(String strCmdClassNames, String strStaticConfigFiles, String wfid, String defaultFs){
		String[] cmdClassNames = strCmdClassNames.split(",");
		String[] staticCfgFiles = strStaticConfigFiles.split(",");
		
		try{
			ETLCmd[] cmds = new ETLCmd[cmdClassNames.length];
			for (int i=0; i<cmds.length; i++){
				cmds[i] = (ETLCmd) Class.forName(cmdClassNames[i]).getConstructor(String.class, String.class, String.class, String[].class).
						newInstance(wfid, staticCfgFiles[i], defaultFs, null);
				cmds[i].setPm(ProcessMode.MRProcess);
			}
			return cmds;
		}catch(Throwable e){
			logger.error("", e);
		}
		return null;
	}
	
	//
	public boolean isSendLog() {
		return sendLog;
	}
	public void setSendLog(boolean sendLog) {
		this.sendLog = sendLog;
	}
	public String getLogTopicName() {
		return logTopicName;
	}
	public void setLogTopicName(String logTopicName) {
		this.logTopicName = logTopicName;
	}
	public String getBootstrapServers() {
		return bootstrapServers;
	}
	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public PropertiesConfiguration getEngineProp() {
		return engineProp;
	}

	public void setEngineProp(PropertiesConfiguration engineProp) {
		this.engineProp = engineProp;
	}
}
