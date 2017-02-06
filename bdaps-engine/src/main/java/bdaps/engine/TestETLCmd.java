package bdaps.engine;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import bdaps.engine.core.ACmd;
import etl.engine.ETLCmd;
import etl.engine.types.InputFormatType;
import scala.Tuple2;

public class TestETLCmd extends ETLCmd{
/*************
 * ACmd Test utils
 */
	public List<String> sparkTestACmd(String remoteInputFolder, String[] inputDataFiles, 
		String cmdProperties, Class<? extends ACmd> cmdClass, InputFormatType ift) throws Exception{
		return sparkTestACmd(remoteInputFolder, inputDataFiles, cmdProperties, cmdClass, ift, false);
	}
	public List<String> sparkTestACmdKeys(String remoteInputFolder, String[] inputDataFiles, 
		String cmdProperties, Class<? extends ACmd> cmdClass, InputFormatType ift) throws Exception{
		return sparkTestACmd(remoteInputFolder, inputDataFiles, cmdProperties, cmdClass, ift, true);
	}
	
	public List<String> sparkTestACmd(String remoteInputFolder, String[] inputDataFiles, 
			String cmdProperties, Class<? extends ACmd> cmdClass, InputFormatType ift, boolean key) throws Exception{
		SparkSession spark = SparkSession.builder().appName("wfName").master("local[5]").getOrCreate();
		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);
		try {
			getFs().delete(new Path(remoteInputFolder), true);
			List<String> inputPaths = new ArrayList<String>();
			for (String inputFile : inputDataFiles) {
				getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + inputFile), new Path(remoteInputFolder + inputFile));
				inputPaths.add(remoteInputFolder + inputFile);
			}
			if (InputFormatType.FileName == ift){
				inputPaths.clear();
				inputPaths.add(remoteInputFolder);
			}
			Constructor<? extends ACmd> constr = cmdClass.getConstructor(String.class, String.class, String.class, String.class, String.class);
			String cfgProperties = cmdProperties;
			if (this.getResourceSubFolder()!=null){
				cfgProperties = this.getResourceSubFolder() + cmdProperties;
			}
			String wfName = "wfName";
			
			ACmd cmd = constr.newInstance(wfName, "wfId", cfgProperties, null, this.getDefaultFS());
			RDD<Tuple2<String, String>> result = cmd.sparkProcessFilesToKV(jsc.parallelize(inputPaths).rdd(), ift, spark);
			
			JavaRDD<Tuple2<String,String>> jresult = result.toJavaRDD();
			if (key){
				List<String> keys = jresult.map(new Function<Tuple2<String,String>, String>(){
					@Override
					public String call(Tuple2<String, String> v1) throws Exception {
						return v1._1;
					}
				}).collect();
				return keys;
			}else{
				List<String> values = jresult.map(new Function<Tuple2<String,String>, String>(){
					@Override
					public String call(Tuple2<String, String> v1) throws Exception {
						return v1._2;
					}
				}).collect();
				return values;
			}
		}finally{
			jsc.close();
		}
	}
}