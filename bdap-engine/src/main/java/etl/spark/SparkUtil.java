package etl.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import bdap.util.HdfsUtil;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;

public class SparkUtil {

	public static final Logger logger = LogManager.getLogger(SparkUtil.class);
	
	/**
	 * @param input, tuple:tableName to one line
	 * @param defaultFs
	 * @param dir
	 * @param wfid
	 * @return tableName to fileName
	 */
	//TODO remove this
	public static void saveByKey(JavaPairRDD<String, String> input, String defaultFs, String dir, String wfid){
		List<Tuple2<String, Iterable<String>>> datalist = input.groupByKey().collect();
		for (Tuple2<String, Iterable<String>> data: datalist){
			String key = data._1;
			Iterable<String> vs = data._2;
			String fileName = String.format("%s%s%s/%s", defaultFs, dir, wfid, key);
			logger.info(String.format("going to save as hadoop file:%s with count %d", fileName, input.count()));
			FileSystem fs = HdfsUtil.getHadoopFs(defaultFs);
			HdfsUtil.writeDfsFile(fs, fileName, vs);
		}
	}
	
	public static JavaRDD<String> fromString(String str, JavaSparkContext jsc){
		return jsc.parallelize(Arrays.asList(new String[]{str}));
	}
	
	public static JavaRDD<String> fromFile(String str, JavaSparkContext jsc){
		return jsc.textFile(str);
	}
	
	public static JavaRDD<String> filterPairRDD(JavaPairRDD<String,String> input, String key){
		return input.filter(new Function<Tuple2<String,String>, Boolean>(){
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				if (key.equals(v1._1)){
					return true;
				}else{
					return false;
				}
			}
		}).map(new Function<Tuple2<String, String>, String>(){
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				return v1._2;
			}
		});
	}
}
