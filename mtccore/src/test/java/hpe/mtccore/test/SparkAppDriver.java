package hpe.mtccore.test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import etl.cmd.CsvAggregateCmd;
import etl.cmd.CsvTransformCmd;
import etl.cmd.SftpCmd;
import etl.cmd.XmlToCsvCmd;
import etl.spark.SparkUtil;

public class SparkAppDriver implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = Logger.getLogger(SparkAppDriver.class);
	
	@Test
	public void sgsiwfFromMsg(){
		try {
			PropertyConfigurator.configure("log4j.properties");
		    Logger log = LogManager.getRootLogger();
		    log.setLevel(Level.DEBUG);
		    
			final String defaultFs = "hdfs://127.0.0.1:19000";
			final String remoteCfg = "/mtccore/etlcfg/";
			final String inputdir = "/mtccore/xmldata/wfid/";
			final String outputdir = "/mtccore/csvdata/csv/";
			
			final String sftpProperties = "sgsiwf.sftp.properties";
			final String xml2csvProperties = "sgsiwf.schemaFromXml.properties";
			final String aggrcsvProperties = "sgsiwf.aggr.properties";
			final String transcsvProperties = "sgsiwf.trans.properties";
			final String csvloadProperties = "sgsiwf.loaddata.properties";
			final String sendLogProperties = "sgsiwf.sendlog.properties";
			final String backupProperties = "sgsiwf.backup.properties";
			
			
			SparkConf conf = new SparkConf().setAppName("mtccore").setMaster("local[3]");
			
			final JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
			
			String groupId = "testgid";
			String topicName = "log-analysis-topic";
			Map<String, Object> kafkaParams = new HashMap<String, Object>();
			kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
			kafkaParams.put("group.id", groupId);
			kafkaParams.put("key.deserializer", StringDeserializer.class);
			kafkaParams.put("value.deserializer", StringDeserializer.class);
			JavaInputDStream<ConsumerRecord<Object, Object>> ds = KafkaUtils.createDirectStream(jsc, 
					LocationStrategies.PreferConsistent(), 
					ConsumerStrategies.Subscribe(Arrays.asList(new String[]{topicName}), kafkaParams));
			
			ds.cache().foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<Object,Object>>, Time>(){
				private static final long serialVersionUID = 1L;
				@Override
				public void call(JavaRDD<ConsumerRecord<Object, Object>> v1, Time v2) throws Exception {
					if (!v1.isEmpty() && v1.count()>0){
						
						String wfid = v2.toString();
						SftpCmd sftpCmd = new SftpCmd(wfid, remoteCfg+sftpProperties, defaultFs, null);
						JavaRDD<Tuple2<String, String>> files = sftpCmd.sparkProcess(null, jsc.sparkContext()).cache();
						
						XmlToCsvCmd xml2csvCmd = new XmlToCsvCmd(wfid, remoteCfg+xml2csvProperties, defaultFs, null);
						JavaRDD<Tuple2<String, String>> csvs = xml2csvCmd.sparkProcess(files, jsc.sparkContext()).cache();
						
						SparkUtil.saveByKey(csvs, defaultFs, outputdir, wfid);
						
						CsvAggregateCmd aggrCmd = new CsvAggregateCmd(wfid, remoteCfg+aggrcsvProperties, defaultFs, null);
						JavaRDD<Tuple2<String, String>> aggrcsvs = aggrCmd.sparkProcess(csvs, jsc.sparkContext());
						
						SparkUtil.saveByKey(aggrcsvs, defaultFs, outputdir, wfid);
						
						CsvTransformCmd transCmd = new CsvTransformCmd(wfid, remoteCfg+transcsvProperties, defaultFs, null);
						JavaRDD<Tuple2<String, String>> transcsvs = transCmd.sparkProcess(aggrcsvs, jsc.sparkContext());
						
						SparkUtil.saveByKey(transcsvs, defaultFs, outputdir, wfid);
						
						
					}
					
				}
			});
			
			jsc.start();
			jsc.awaitTermination();
			jsc.close();
		}catch(Exception e){
			logger.error("", e);
		}
	}
}
