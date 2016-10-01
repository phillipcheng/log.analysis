package hpe.mtccore.test;

import java.io.Serializable;
import org.junit.Test;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import etl.cmd.BackupCmd;
import etl.cmd.CsvAggregateCmd;
import etl.cmd.CsvTransformCmd;
import etl.cmd.LoadDataCmd;
import etl.cmd.SchemaFromXmlCmd;
import etl.cmd.XmlToCsvCmd;
import etl.engine.EngineUtil;
import etl.spark.CmdReciever;
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
			final String outputdir = "/mtccore/csvdata/csv/";
			
			final String sftpProperties = "sgsiwf.sftp.properties";
			final String xml2csvProperties = "sgsiwf.schemaFromXml.properties";
			final String aggrcsvProperties = "sgsiwf.aggr.properties";
			final String transcsvProperties = "sgsiwf.trans.properties";
			final String csvloadProperties = "sgsiwf.loaddata.spark.properties";
			final String sendLogProperties = "sgsiwf.sendlog.properties";
			final String backupProperties = "sgsiwf.backup.properties";
			
			SparkConf conf = new SparkConf().setAppName("mtccore").setMaster("local[3]");
			
			final JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
			final String wfName  = "sgsiwf";
			String recieverClassName = "etl.cmd.SftpCmd";
			/*
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
			*/
			JavaDStream<String> ds = jsc.receiverStream(new CmdReciever(StorageLevel.MEMORY_ONLY(), recieverClassName, wfName, 
					remoteCfg+sftpProperties, defaultFs, 6000));
			ds.cache().foreachRDD(new VoidFunction2<JavaRDD<String>, Time>(){
				private static final long serialVersionUID = 1L;
				public void call(JavaRDD<String> v1, Time v2) throws Exception {
					if (!v1.isEmpty() && v1.count()>0){
						
						String first = v1.first();
						String wfid = first.substring(0, first.indexOf(CmdReciever.WFID_SEP));
						
						SchemaFromXmlCmd schemaFromXmlCmd = new SchemaFromXmlCmd(wfName, wfid, 
								remoteCfg+xml2csvProperties, defaultFs, null);
						EngineUtil.getInstance().processJavaCmd(schemaFromXmlCmd);
						
						XmlToCsvCmd xml2csvCmd = new XmlToCsvCmd(wfName, wfid, remoteCfg+xml2csvProperties, defaultFs, null);
						JavaRDD<Tuple2<String, String>> csvs = xml2csvCmd.sparkProcess(v1).cache();
						
						SparkUtil.saveByKey(csvs, defaultFs, outputdir, wfid);
						
						CsvAggregateCmd aggrCmd = new CsvAggregateCmd(wfName, wfid, remoteCfg+aggrcsvProperties, defaultFs, null);
						EngineUtil.getInstance().processJavaCmd(aggrCmd);
						JavaRDD<Tuple2<String, String>> aggrcsvs = aggrCmd.sparkProcessKeyValue(csvs);
						
						SparkUtil.saveByKey(aggrcsvs, defaultFs, outputdir, wfid);
						
						CsvTransformCmd transCmd = new CsvTransformCmd(wfName, wfid, remoteCfg+transcsvProperties, defaultFs, null);
						EngineUtil.getInstance().processJavaCmd(transCmd);
						JavaRDD<Tuple2<String, String>> transcsvs = transCmd.sparkProcessKeyValue(aggrcsvs);
						
						SparkUtil.saveByKey(transcsvs, defaultFs, outputdir, wfid);
						
						LoadDataCmd loadDataCmd = new LoadDataCmd(wfName, wfid, remoteCfg+csvloadProperties, defaultFs, null);
						EngineUtil.getInstance().processJavaCmd(loadDataCmd);
						
						BackupCmd backupCmd = new BackupCmd(wfName, wfid, remoteCfg+backupProperties, defaultFs, null);
						EngineUtil.getInstance().processJavaCmd(backupCmd);
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
