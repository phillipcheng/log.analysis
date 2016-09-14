package etl.spark;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import etl.util.Util;
import scala.Tuple2;

public class SparkUtil {

	public static final Logger logger = Logger.getLogger(SparkUtil.class);
	
	public static void saveByKey(JavaRDD<Tuple2<String, String>> input, final String defaultFs, 
			final String dir, String wfid){
		JavaPairRDD<String, String> pairs = input.mapToPair(new PairFunction<Tuple2<String, String>, String, String>(){
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				return t;
			}
		});
		
		List<Tuple2<String, Iterable<String>>> datalist = pairs.groupByKey().collect();
		for (Tuple2<String, Iterable<String>> data: datalist){
			String key = data._1;
			Iterable<String> vs = data._2;
			String fileName = String.format("%s%s%s/%s", defaultFs, dir, wfid, key);
			logger.info(String.format("going to save as hadoop file:%s with count %d", fileName, input.count()));
			FileSystem fs = Util.getHadoopFs(defaultFs);
			Util.writeDfsFile(fs, fileName, vs);
		}
	}
}
