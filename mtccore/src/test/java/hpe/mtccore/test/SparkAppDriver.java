package hpe.mtccore.test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import scala.Tuple3;

import etl.cmd.CsvAggregateCmd;
import etl.cmd.CsvTransformCmd;
import etl.cmd.SftpCmd;
import etl.cmd.XmlToCsvCmd;
import etl.spark.SparkUtil;
import etl.spark.TableEqualsFilterFun;
import etl.spark.TableNotEqualsFilterFun;
import kafka.serializer.StringDecoder;
import mtccore.sgsiwf.SGSIWFFlow;

public class SparkAppDriver implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = Logger.getLogger(SparkAppDriver.class);
	
	@Test
	public void sgsiwfStreaming(){
		SGSIWFFlow.main(null);
	}
	
	@Test
	public void sgsiwfFromMsg(){
		try {
			PropertyConfigurator.configure("log4j.properties");
		    Logger log = LogManager.getRootLogger();
		    log.setLevel(Level.INFO);
		    
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
			
			
			SparkConf conf = new SparkConf().setAppName("mtccore").setMaster("local[2]");
			
			JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
			
			String topicName = "log-analysis-topic";
			Set<String> topicsSet = new HashSet<String>(Arrays.asList(new String[]{topicName}));
			Map<String, String> kafkaParams = new HashMap<String, String>();
			kafkaParams.put("metadata.broker.list", "127.0.0.1:9092");
			kafkaParams.put("group.id", "test");
			
			
			    
			JavaPairInputDStream<String, String> ds = KafkaUtils.createDirectStream(jsc, 
					String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
			
			/*
			String groupId = "testgid";
			Map<String, Integer> topicMap = new HashMap<>();
			topicMap.put(topicName, 3);
			String zkQuorum = "localhost";
		    
			JavaPairReceiverInputDStream<String, String> ds = 
					KafkaUtils.createStream(jsc, zkQuorum, groupId, topicMap);
			*/
			
			final AtomicReference<String> wfid = new AtomicReference<String>();
			ds.print();
			
			//timestamp evt to start a new batch
			JavaPairDStream<String, String> files = ds.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					logger.info(String.format("recieved message %s", t));
					wfid.set(t._2);
					SftpCmd sftpCmd = new SftpCmd(wfid.get(), remoteCfg + sftpProperties, defaultFs, null);
					return sftpCmd.flatMapToPair(t._1, t._2).iterator();
				}
			});
			
			final XmlToCsvCmd xml2csvCmd = new XmlToCsvCmd(wfid.get(), remoteCfg + xml2csvProperties, defaultFs, null);
			JavaPairDStream<String, String> csvret = files.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					logger.info(String.format("going to process message %s", t));
					return xml2csvCmd.flatMapToPair(t._1, t._2).iterator();
				}
			});
			
			final CsvAggregateCmd aggrCmd = new CsvAggregateCmd(wfid.get(), remoteCfg + aggrcsvProperties, defaultFs, null);
			TableEqualsFilterFun aggrff = new TableEqualsFilterFun(aggrCmd.getOldTables());
			
			SparkUtil.saveByKey(csvret, defaultFs, outputdir);
			
			//tableKey.aggrKeys,aggrValues
			JavaPairDStream<String, String> csvgroup = csvret.filter(aggrff).flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					return aggrCmd.flatMapToPair(t._1, t._2).iterator();
				}
			});
			
			//tableName, csv
			JavaPairDStream<String, String> csvaggr = csvgroup.groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<String>>,String,String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
					Tuple3<String, String, String> t3 = aggrCmd.reduceByKey(t._1, t._2);
					return new Tuple2<String, String>(t3._3(), t3._1()+","+t3._2());
				}
			});
			
			final CsvTransformCmd transCmd = new CsvTransformCmd(wfid.get(), remoteCfg + transcsvProperties, defaultFs, null);
			TableEqualsFilterFun transff = new TableEqualsFilterFun(new String[]{transCmd.getOldTable()});
			TableNotEqualsFilterFun ntransff = new TableNotEqualsFilterFun(new String[]{transCmd.getOldTable()});
			SparkUtil.saveByKey(csvaggr.filter(ntransff), defaultFs, outputdir);
			
			JavaPairDStream<String, String> csvtrans = csvaggr.filter(transff).flatMapToPair(new PairFlatMapFunction<Tuple2<String,String>,String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					return transCmd.flatMapToPair(t._1, t._2).iterator();
				}
			});
			SparkUtil.saveByKey(csvtrans, defaultFs, outputdir);
			
			jsc.start();
			jsc.awaitTermination();
			jsc.close();
		}catch(Exception e){
			logger.error("", e);
		}
	}
}
