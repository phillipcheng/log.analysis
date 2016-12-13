package etl.spark;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface SparkProcessor {
	/**
	 * kv to kv
	 * @param input
	 * @return
	 */
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc);
	
	/**
	 * v to v
	 * @param input
	 * @return
	 */
	public JavaRDD<String> sparkProcess(JavaRDD<String> input, JavaSparkContext jsc);
	
	/**
	 * v to kv
	 * @param input
	 * @return
	 */
	public JavaPairRDD<String, String> sparkProcessV2KV(JavaRDD<String> input, JavaSparkContext jsc);
}
