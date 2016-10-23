package etl.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public interface SparkProcessor {
	/**
	 * 
	 * @param input
	 * @return
	 */
	public JavaRDD<Tuple2<String, String>> sparkProcessKeyValue(JavaRDD<Tuple2<String, String>> input);
	
	/**
	 * 
	 * @param input
	 * @return
	 */
	public JavaRDD<Tuple2<String, String>> sparkProcess(JavaRDD<String> input);
}
