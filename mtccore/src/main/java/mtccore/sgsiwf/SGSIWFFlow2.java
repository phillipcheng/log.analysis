package mtccore.sgsiwf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.PreferConsistent;

import etl.cmd.CsvAggregateCmd;
import etl.cmd.CsvTransformCmd;
import etl.cmd.SftpCmd;
import etl.cmd.XmlToCsvCmd;
import etl.spark.SparkUtil;
import etl.spark.TableEqualsFilterFun;
import etl.spark.TableNotEqualsFilterFun;
import etl.util.FilenameInputFormat;
import scala.Tuple2;
import scala.Tuple3;

public class SGSIWFFlow2 {
	
	public static final Logger logger = Logger.getLogger(SGSIWFFlow2.class);
	
	public static void main(String args[]){
		try {
		    
		}catch(Exception e){
			logger.error("", e);
		}
	}
}
