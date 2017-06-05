package etl.util;

import java.util.Arrays;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import etl.engine.types.DataType;
import etl.input.FilenameTextInputFormat;
import scala.Tuple2;

//methods needed for generated Spark Driver code
public class SparkUtil {

	public static final Logger logger = LogManager.getLogger(SparkUtil.class);
	
	public static JavaRDD<String> fromString(String str, JavaSparkContext jsc){
		return jsc.parallelize(Arrays.asList(new String[]{str}));
	}
	
	//return the file path the key, line as value
	public static JavaPairRDD<String, String> fromFileKeyValue(String paths, JavaSparkContext jsc, Configuration conf, String baseOutput){
		return jsc.newAPIHadoopFile(paths, FilenameTextInputFormat.class, Text.class, Text.class, conf).mapToPair(
				new PairFunction<Tuple2<Text, Text>, String, String>(){
			@Override
			public Tuple2<String, String> call(Tuple2<Text, Text> t) throws Exception {
				if (baseOutput!=null){
					return new Tuple2<String, String>(baseOutput, t._2.toString());
				}else{
					return new Tuple2<String, String>(t._1.toString(), t._2.toString());
				}
			}
		});
	}
	
	//
	public static JavaPairRDD<String, String> filterPairRDD(JavaPairRDD<String,String> input, String key){
		return input.filter(new Function<Tuple2<String,String>, Boolean>(){
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				if (key.equals(v1._1)){
					return true;
				}else{
					return false;
				}
			}
		});
	}
	
	public static String getCmdSparkProcessMethod(DataType inputDataType, DataType outputDataType){
		String methodName = null;
		if (inputDataType==DataType.Path && 
				(outputDataType==DataType.KeyPath || outputDataType==DataType.KeyValue)){
			methodName = "sparkProcessFilesToKV";
		}else{
			methodName = "sparkProcessKeyValue";
		}
		return methodName;
	}
	
	public static JavaPairRDD<String, String> flip(JavaPairRDD<String, String> input){
		return input.mapToPair(new PairFunction<Tuple2<String, String>, String, String>(){
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<String, String>(t._2, t._1);
			}
		});
	}
}
