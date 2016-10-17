package etl.engine;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import scala.Tuple2;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.log.ETLLog;
import etl.log.LogType;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class EngineUtil {
	public static final Logger logger = LogManager.getLogger(EngineUtil.class);
	
	public static String config_file="etlengine.properties";
	
	public static final String key_defaultfs="defaultFs";
	public static final String key_bootstrap_servers="kafka.bootstrap.servers";
	public static final String key_kafka_log_topic="kafka.log.topic";
	public static final String key_enable_kafka="kafka.enabled";
	public static final String key_log_interval="log.interval";//seconds
	
	private PropertiesConfiguration engineProp = null;
	
	private Producer<String, String> producer = null;
	
	private String defaultFs;
	private String logTopicName;
	private String bootstrapServers;
	private boolean sendLog=false; //engine level send log flag
	private int logInterval=30;

	public static void setConfFile(String file){
		config_file = file;
		singleton = new EngineUtil();
	}
	
	public Producer<String, String> getProducer(){
		return producer;
	}
	
	private Producer<String, String> createProducer(String bootstrapServers){
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
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
		consumer.subscribe(Arrays.asList(topics));
		return consumer;
	}
	
	private EngineUtil(){
		InputStream input = this.getClass().getClassLoader().getResourceAsStream(config_file);
		if (input!=null){
			try {
				engineProp = new PropertiesConfiguration();
				engineProp.load(input);
				setDefaultFs(engineProp.getString(key_defaultfs));
				logTopicName = engineProp.getString(key_kafka_log_topic);
				sendLog = engineProp.getBoolean(key_enable_kafka, false);
				bootstrapServers = engineProp.getString(key_bootstrap_servers);
				logger.info(String.format("kafka producer bootstrap servers:%s", bootstrapServers));
				producer = createProducer(bootstrapServers);
				logInterval = engineProp.getInt(key_log_interval, 30);
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
	
	public void sendLog(String topicName, ETLLog etllog){
		sendMsg(topicName, etllog.getType().toString(), etllog.toString());
	}
	
	public void sendMsg(String topicName, String key, String msg){
		if (producer!=null){
			logger.info(String.format("kafka produce msg: key:%s, msg:%s", key, msg));
			producer.send(new ProducerRecord<String,String>(topicName, key, msg), new Callback(){
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e!=null){
						logger.error("exception got while send log", e);
					}
				}
			});
		}else{
			logger.error("producer is null.");
		}
	}
	
	public void sendLog(ETLLog etllog){
		sendLog(this.logTopicName, etllog);
	}
	
	public ETLLog getETLLog(ETLCmd cmd, Date startTime, Date endTime, List<String> loginfo){
		ETLLog etllog = new ETLLog(LogType.etlstat);
		etllog.setStart(startTime);
		etllog.setEnd(endTime);
		etllog.setWfName(cmd.getWfName());
		etllog.setWfid(cmd.getWfid());
		etllog.setActionName(cmd.getClass().getName());
		etllog.setCounts(loginfo);
		return etllog;
	}
	
	public void sendCmdLog(ETLCmd cmd, Date startTime, Date endTime, List<String> loginfo){
		if (cmd.isSendLog() && this.isSendLog()){
			ETLLog etllog = getETLLog(cmd, startTime, endTime, loginfo);
			sendLog(this.logTopicName, etllog);
		}
	}
	
	
	public ETLCmd[] getCmds(String strCmdClassNames, String strStaticConfigFiles, String wfName, String wfid, String defaultFs, 
			String[] otherArgs, ProcessMode pm){
		String[] cmdClassNames = strCmdClassNames.split(",", -1);
		String[] staticCfgFiles = strStaticConfigFiles.split(",", -1);
		ETLCmd[] cmds = new ETLCmd[cmdClassNames.length];
		if (cmdClassNames.length!=staticCfgFiles.length){
			String msg =String.format("cmdClass %s and staticCfg %s not matching.", strCmdClassNames, strStaticConfigFiles);
			logger.error(new ETLLog(wfName, wfid, strCmdClassNames, msg, null));
		}
		
		for (int i=0; i<cmds.length; i++){
			try {
				cmds[i] = (ETLCmd) Class.forName(cmdClassNames[i]).getConstructor(String.class, String.class, String.class, String.class, String[].class).
						newInstance(wfName, wfid, staticCfgFiles[i], defaultFs, otherArgs);
				cmds[i].setPm(pm);
			}catch(Throwable t){
				if (cmds[i]!=null){
					logger.error(new ETLLog(cmds[i], null, t), t);
				}else{
					logger.error("", t);
				}
				return null;
			}
		}
		return cmds;
	}
	
	public ETLCmd getCmd(String cmdClassName, String configFile, String wfName, String wfid, String defaultFs, 
			String[] otherArgs, ProcessMode pm){
		ETLCmd cmd = null;
		try {
			cmd = (ETLCmd) Class.forName(cmdClassName).getConstructor(String.class, String.class, String.class, 
					String.class, String[].class).newInstance(wfName, wfid, configFile, defaultFs, otherArgs);
			cmd.setPm(pm);
		}catch(Throwable t){
			if (cmd!=null){
				logger.error(new ETLLog(cmd, null, t), t);
			}else{
				logger.error("", t);
			}
		}
		return cmd;
	}
	
	public void processReduceCmd(ETLCmd cmd, Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) {
		try {
			List<String[]> rets = cmd.reduceProcess(key, values);
			for (String[] ret: rets){
				if (ETLCmd.SINGLE_TABLE.equals(ret[2])){
					if (ret[1]!=null){
						context.write(new Text(ret[0]), new Text(ret[1]));
					}else{
						context.write(new Text(ret[0]), null);
					}
				}else{
					if (ret[1]!=null){
						mos.write(new Text(ret[0]), new Text(ret[1]), ret[2]);
					}else{
						mos.write(new Text(ret[0]), null, ret[2]);
					}
				}
			}
		}catch(Throwable t){
			logger.error(new ETLLog(cmd, null, t), t);
		}
	}

	public void processJavaCmd(ETLCmd cmd){
		Date startTime = new Date();
		List<String> logoutputs = cmd.sgProcess();
		Date endTime = new Date();
		sendCmdLog(cmd, startTime, endTime, logoutputs);
	}
	
	public void processMapperCmds(ETLCmd[] cmds, long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context) {
		String input = row;
		for (int i=0; i<cmds.length; i++){
			ETLCmd cmd = cmds[i];
			try {
				Date startTime = new Date();
				Map<String, Object> alloutputs = cmd.mapProcess(offset, input, context);
				Date endTime = new Date();
				if (alloutputs!=null){
					if (cmd.getMrMode()==MRMode.file){
						//generate log for file mode mr processing
						List<String> logoutputs = (List<String>) alloutputs.get(ETLCmd.RESULT_KEY_LOG);
						sendCmdLog(cmd, startTime, endTime, logoutputs);
					}
					if (alloutputs.containsKey(ETLCmd.RESULT_KEY_OUTPUT_LINE)){
						//for all mapper only cmd, the result should contains the RESULT_KEY_OUTPUT
						List<String> outputs = (List<String>) alloutputs.get(ETLCmd.RESULT_KEY_OUTPUT_LINE);
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
					}else if (alloutputs.containsKey(ETLCmd.RESULT_KEY_OUTPUT_TUPLE2)){//for map-reduce mapper phrase, the result is key-value pair
						if (alloutputs!=null && context!=null){
							List<Tuple2<String, String>> tl = (List<Tuple2<String, String>>) alloutputs.get(ETLCmd.RESULT_KEY_OUTPUT_TUPLE2);
							for (Tuple2<String, String> kv: tl){
								if (kv!=null){
									context.write(new Text(kv._1), new Text(kv._2));
								}
							}
						}
					}else{
						logger.info("no output.");
					}
				}
			}catch(Throwable t){
				logger.error(new ETLLog(cmd, null, t), t);
			}
		}
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

	public String getDefaultFs() {
		return defaultFs;
	}

	public void setDefaultFs(String defaultFs) {
		this.defaultFs = defaultFs;
	}

	public int getLogInterval() {
		return logInterval;
	}

	public void setLogInterval(int logInterval) {
		this.logInterval = logInterval;
	}
}
