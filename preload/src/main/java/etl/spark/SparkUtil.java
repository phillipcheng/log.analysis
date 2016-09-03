package etl.spark;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import etl.util.Util;
import scala.Tuple2;

public class SparkUtil {

	public static final Logger logger = Logger.getLogger(SparkUtil.class);
	
	public static void saveByKey(JavaPairDStream<String, String> dstream, final String defaultFs, String dir){
		dstream.groupByKey().foreachRDD(new VoidFunction2<JavaPairRDD<String, Iterable<String>>, Time>(){
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaPairRDD<String, Iterable<String>> v1, Time v2) throws Exception {
				if (!v1.partitions().isEmpty() && v1.count()>0){
					List<Tuple2<String, Iterable<String>>> datalist = v1.collect();
					for (Tuple2<String, Iterable<String>> data: datalist){
						String key = data._1;
						Iterable<String> vs = data._2;
						String fileName = String.format("%s%s%d/%s", defaultFs, dir, v2.milliseconds(), key);
						logger.info(String.format("going to save as hadoop file:%s with count %d", fileName, v1.count()));
						FileSystem fs = Util.getHadoopFs(defaultFs);
						Util.writeDfsFile(fs, fileName, vs);
					}
				}
			}
		});
	}
}
