package mtccore.sgsiwf;

import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import etl.cmd.CsvAggregateCmd;
import etl.cmd.CsvTransformCmd;
import etl.cmd.XmlToCsvCmd;
import etl.spark.SparkUtil;
import etl.spark.TableEqualsFilterFun;
import etl.spark.TableNotEqualsFilterFun;
import etl.util.FilenameInputFormat;
import scala.Tuple2;
import scala.Tuple3;

public class SGSIWFFlow {
	
	public static final Logger logger = Logger.getLogger(SGSIWFFlow.class);
	
	public static void main(String args[]){
		try {
			
			final String wfid="dummy";
			final String defaultFs = "hdfs://127.0.0.1:19000";
			final String remoteCfg = "/mtccore/etlcfg/";
			final String inputdir = "/mtccore/xmldata/wfid/";
			final String outputdir = "/mtccore/csvdata/csv/";
			
			final String xml2csvProperties = "sgsiwf.schemaFromXml.properties";
			final String aggrcsvProperties = "sgsiwf.aggr.properties";
			final String transcsvProperties = "sgsiwf.trans.properties";
			final String csvloadProperties = "sgsiwf.loaddata.properties";
			
			final XmlToCsvCmd xml2csvCmd = new XmlToCsvCmd(wfid, remoteCfg + xml2csvProperties, defaultFs, null);
			final CsvAggregateCmd aggrCmd = new CsvAggregateCmd(wfid, remoteCfg + aggrcsvProperties, defaultFs, null);
			final CsvTransformCmd transCmd = new CsvTransformCmd(wfid, remoteCfg + transcsvProperties, defaultFs, null);
			
			SparkConf conf = new SparkConf().setAppName("mtccore").setMaster("local[3]");
			
			JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
			JavaPairInputDStream<LongWritable, Text> ds = jsc.fileStream(
					defaultFs+inputdir, LongWritable.class, Text.class, FilenameInputFormat.class);
			//tableName to csv
			JavaPairDStream<String, String> csvret = ds.flatMapToPair(new PairFlatMapFunction<Tuple2<LongWritable, Text>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<LongWritable, Text> t) throws Exception {
					return xml2csvCmd.flatMapToPair(String.valueOf(t._1), t._2.toString());
				}
			});
			
			TableEqualsFilterFun aggrff = new TableEqualsFilterFun(aggrCmd.getOldTables());
			
			SparkUtil.saveByKey(csvret, defaultFs, outputdir);
			
			//tableKey.aggrKeys,aggrValues
			JavaPairDStream<String, String> csvgroup = csvret.filter(aggrff).flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					return aggrCmd.flatMapToPair(t._1, t._2);
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
			
			TableEqualsFilterFun transff = new TableEqualsFilterFun(new String[]{transCmd.getOldTable()});
			TableNotEqualsFilterFun ntransff = new TableNotEqualsFilterFun(new String[]{transCmd.getOldTable()});
			SparkUtil.saveByKey(csvaggr.filter(ntransff), defaultFs, outputdir);
			
			JavaPairDStream<String, String> csvtrans = csvaggr.filter(transff).flatMapToPair(new PairFlatMapFunction<Tuple2<String,String>,String, String>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
					return transCmd.flatMapToPair(t._1, t._2);
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
