package etl.flow.spark;

import java.util.List;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.flow.Flow;
import etl.flow.Node;

public class SparkGenerator {
	
	private static String getPackageName(String prjName){
		return prjName.replaceAll("\\.", "_").replaceAll("-", "_");
	}
	
	private static String getClassName(String flowName){
		return flowName;
	}
	
	public static void genDriverJava(String prjName, Flow flow, String srcRootDir, SparkServerConf ssc) throws Exception{
		String packageName = getPackageName(prjName);
		String className = getClassName(flow.getName());
		Path folder = Paths.get(String.format("%s%s%s", srcRootDir, File.separator, packageName));
		if (!Files.exists(folder)){
			Files.createDirectories(folder);
		}
		Path file = folder.resolve(Paths.get(String.format("%s.java", getClassName(flow.getName()))));
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("package %s;\n", packageName));
		sb.append("import java.util.*;\n");
		sb.append("import org.apache.log4j.Logger;\n");
		sb.append("import org.apache.spark.SparkConf;\n");
		sb.append("import org.apache.spark.api.java.JavaPairRDD;\n");
		sb.append("import org.apache.spark.api.java.JavaRDD;\n");
		sb.append("import org.apache.spark.api.java.JavaSparkContext;\n");
		//begin class
		sb.append(String.format("public class %s extends %s implements %s {\n", className, ETLCmd.class.getName(), Serializable.class.getName()));
		sb.append(String.format("public static final Logger logger = Logger.getLogger(%s.class);\n", className));
		//gen constructor
		sb.append(String.format("public %s(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){\n", className));
		sb.append("init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);}\n");
		sb.append("public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){\n");
		sb.append("super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);}\n");
		//gen sgProcess
		sb.append("public List<String> sgProcess() {\n");
		sb.append("List<String> retInfo = new ArrayList<String>();\n");
		sb.append(String.format("SparkConf conf = new SparkConf().setAppName(getWfName());\n"));
		sb.append("JavaSparkContext jsc = new JavaSparkContext(conf);\n");
		//gen cmds
		List<Node> nodes = flow.getTopoOrder();
		sb.append("jsc.close();\n");
		sb.append("return retInfo;");
		sb.append("}\n");
		//end class
		sb.append(String.format("}\n"));
		Files.write(file, sb.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
	}
}
