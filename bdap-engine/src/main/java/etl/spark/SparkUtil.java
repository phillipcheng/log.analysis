package etl.spark;

import java.util.ArrayList;
import java.util.List;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaPairRDD;

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
}
