package etl.spark;

import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

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
	 * v to Map of k, rdd<v>
	 * @param input
	 * @return
	 */
	public Map<String, JavaRDD<String>> sparkSplitProcess(JavaRDD<String> input, JavaSparkContext jsc);
	
	/**
	 * v to Map of k, rdd<v>
	 * @param input
	 * @return
	 */
	public JavaPairRDD<String, String> sparkVtoKvProcess(JavaRDD<String> input, JavaSparkContext jsc);
	
}
