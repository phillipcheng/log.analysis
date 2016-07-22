package etl.engine;

import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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
	
	private PropertiesConfiguration engineProp = new PropertiesConfiguration();
	
	private Producer<String, String> producer = null;
	private String logTopicName;
	private boolean enableMessage=false;
	
	public static void setConfFile(String file){
		config_file = file;
		singleton = new EngineUtil();
	}
	
	public Producer<String, String> getProducer(){
		return producer;
	}
	
	private EngineUtil(){
		InputStream input = this.getClass().getClassLoader().getResourceAsStream(config_file);
		if (input!=null){
			try {
				engineProp.load(input);
				logTopicName = engineProp.getString(key_kafka_log_topic);
				enableMessage = engineProp.getBoolean(key_enable_kafka, false);
				
				if (enableMessage){
					Properties props = new Properties();
					props.put("bootstrap.servers", engineProp.getProperty(key_bootstrap_servers));
					props.put("acks", "all");
					props.put("retries", 0);
					props.put("request.timeout.ms", 5000);
					props.put("batch.size", 16384);
					props.put("linger.ms", 1);
					props.put("buffer.memory", 33554432);
					props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
					props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
					producer = new KafkaProducer<String, String>(props);
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
	
	public void sendLog(ETLLog etllog){
		if (producer!=null){
			String value = etllog.toString();
			logger.info(String.format("log: %s", value));
			producer.send(new ProducerRecord<String,String>(this.logTopicName, value), new Callback(){
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e!=null){
						logger.error("", e);
					}
				}
			});
		}
	}
	
	public void sendLog(ETLCmd cmd, Date startTime, Date endTime, List<String> loginfo){
		ETLLog etllog = new ETLLog();
		etllog.setStart(startTime);
		etllog.setEnd(endTime);
		if (cmd.getWfid()!=null) {
			etllog.setWfid(cmd.getWfid());
		}
		etllog.setActionName(cmd.getClass().getName());
		etllog.setCounts(loginfo);
		sendLog(etllog);
	}
	
	public void processMapperCmds(ETLCmd[] cmds, long offset, String row, 
			Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws Exception {
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
										context.write(new Text(line), NullWritable.get());
									}
								}else{
									logger.debug(String.format("final output:%s", outputs));
								}
							}
						}
					}else{
						logger.error("wrong mapper used, for key,value output please use InvokeReducerMapper");
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
	
	//
	public void processReducerMapperCmds(ETLCmd[] cmds, long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		String input = row;
		ETLCmd cmd = cmds[0];
		Map<String, String> alloutputs = cmd.reduceMapProcess(offset, input, context);
		if (alloutputs!=null && context!=null){
			for (String key:alloutputs.keySet()){
				context.write(new Text(key), new Text(alloutputs.get(key)));
			}
		}
	}
	
	public static ETLCmd[] getCmds(String strCmdClassNames, String strStaticConfigFiles, String wfid, String defaultFs){
		String[] cmdClassNames = strCmdClassNames.split(",");
		String[] staticCfgFiles = strStaticConfigFiles.split(",");
		
		try{
			ETLCmd[] cmds = new ETLCmd[cmdClassNames.length];
			for (int i=0; i<cmds.length; i++){
				cmds[i] = (ETLCmd) Class.forName(cmdClassNames[i]).getConstructor(String.class, String.class, String.class, String.class).
						newInstance(wfid, staticCfgFiles[i], null, defaultFs);
				cmds[i].setPm(ProcessMode.MRProcess);
			}
			return cmds;
		}catch(Throwable e){
			logger.error("", e);
		}
		return null;
	}
}
