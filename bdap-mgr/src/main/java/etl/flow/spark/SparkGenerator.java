package etl.flow.spark;

import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


import bdap.util.JavaCodeGenUtil;
import etl.engine.ETLCmd;
import etl.engine.types.DataType;
import etl.engine.types.InputFormatType;
import etl.flow.ActionNode;
import etl.flow.Data;
import etl.flow.Flow;
import etl.flow.Node;
import etl.flow.NodeLet;
import etl.flow.mgr.FlowMgr;
import etl.util.SparkUtil;

public class SparkGenerator {
	public static final Logger logger = LogManager.getLogger(SparkGenerator.class);
	
	public static String getFQClassName(String prjName, String flowName){
		return String.format("%s.%s", getPackageName(prjName), getClassName(flowName));
	}
	
	private static String getPackageName(String prjName){
		return prjName.replaceAll("\\.", "_").replaceAll("-", "_");
	}
	
	private static String getClassName(String flowName){
		return flowName;
	}
	
	public static void genDriverJava(String prjName, Flow flow, String srcRootDir, SparkServerConf ssc) throws Exception{
		flow.init();
		String packageName = getPackageName(prjName);
		String className = getClassName(flow.getName());
		Path folder = Paths.get(String.format("%s%s%s", srcRootDir, File.separator, packageName));
		if (!Files.exists(folder)){
			Files.createDirectories(folder);
		}
		Path file = folder.resolve(Paths.get(String.format("%s.java", getClassName(flow.getName()))));
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("//%s\n", new Date()));
		sb.append(String.format("package %s;\n", packageName));
		sb.append("import java.util.*;\n");
		sb.append("import org.apache.log4j.Logger;\n");
		sb.append("import org.apache.spark.SparkConf;\n");
		sb.append("import org.apache.spark.SparkContext;\n");
		sb.append("import org.apache.spark.api.java.JavaPairRDD;\n");
		sb.append("import org.apache.spark.api.java.JavaRDD;\n");
		sb.append("import org.apache.spark.api.java.JavaSparkContext;\n");
		sb.append("import org.apache.spark.sql.SparkSession;\n");
		//begin class
		sb.append(String.format("public class %s extends %s implements %s {\n", className, ETLCmd.class.getName(), Serializable.class.getName()));
		sb.append(String.format("public static final Logger logger = Logger.getLogger(%s.class);\n", className));
		//declare two masterUrl and resFolder to be used in unit test
		sb.append("private String masterUrl=null;\n");
		sb.append("private String resFolder=\"\";\n");
		sb.append("public void setResFolder(String resFolder) {this.resFolder = resFolder;}\n");
		sb.append("public void setMasterUrl(String masterUrl) {this.masterUrl = masterUrl;}\n");
		//gen constructor
		sb.append(String.format("public %s(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, etl.engine.types.ProcessMode pm){\n", className));
		sb.append("init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);}\n");
		//gen sgProcess
		sb.append("public List<String> sgProcess() {\n");
		sb.append("List<String> retInfo = new ArrayList<String>();\n");
		//create spark context and set master url if not null
		sb.append("SparkConf conf = new SparkConf().setAppName(getWfName());\n");
		sb.append("if (masterUrl!=null) conf.setMaster(masterUrl);\n");
		sb.append("SparkSession spark = SparkSession.builder().config(conf).getOrCreate();\n");
		sb.append("JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());\n");
		//gen cmds
		List<Node> nodes = flow.getActionTopoOrder();
		List<Data> datas = flow.getData();
		for (Data data:datas){
			String varName = JavaCodeGenUtil.getVarName(data.getName());
			String varType = "JavaPairRDD<String,String>";
			String initValue="null";
			if (!data.isInstance()){
				if (data.getBaseOutput()!=null){
					initValue = String.format("etl.util.SparkUtil.fromFileKeyValue(this.getDefaultFs() + \"%s\", jsc, getHadoopConf(), \"%s\")", 
						data.getLocation(), data.getBaseOutput());
				}else{
					initValue = String.format("etl.util.SparkUtil.fromFileKeyValue(this.getDefaultFs() + \"%s\", jsc, getHadoopConf(), null)", 
							data.getLocation());
				}
			}
			//variable declaration line
			sb.append(String.format("%s %s=%s;\n", varType, varName, initValue));
		}
		for (Node node:nodes){
			if (node instanceof ActionNode){
				ActionNode anode = (ActionNode)node;
				String cmdClass = (String) anode.getProperty(ActionNode.key_cmd_class);
				String propertyFileName = FlowMgr.getPropFileName(anode.getName());
				String cmdVarName = anode.getName() + "Cmd";
				//cmd declaration line
				//SftpCmd sftpCmd = new SftpCmd(getWfName(), getWfid(), resFolder + "action_sftp.properties", super.getDefaultFs(), null);
				sb.append(String.format("%s %s=new %s(getWfName(), getWfid(), resFolder + \"%s\", super.getDefaultFs(), null);\n", 
						cmdClass, cmdVarName, cmdClass, propertyFileName));
				String outputVarName =null;
				boolean multiOutput=false;
				DataType outputDataType = null;
				if (node.getOutlets().size()>1){//multiple outlet node, auto-generate an out data variable for filtering
					outputVarName = anode.getName() + "Output";
					multiOutput=true;
					outputDataType = DataType.KeyValue;
					//additional output variable declaration
					sb.append(String.format("%s %s=%s;\n", "JavaPairRDD<String,String>", outputVarName, "null"));
				}else if (node.getOutlets().size()==1){
					NodeLet outlet = node.getOutlets().get(0);
					Data outData = flow.getDataDef(outlet.getDataName());
					outputVarName = JavaCodeGenUtil.getVarName(outlet.getDataName());
					outputDataType = outData.getRecordType();
				}else{//multiple output, the type can only be KeyValue
					outputDataType = DataType.KeyValue;//default
				}
				//cmd execution line
				//data1trans = d1csvTransformCmd.sparkProcessFilesToKV(data1, jsc, TextInputFormat.class);
				String inputVarName=null;
				for (int i=0; i<node.getInLets().size(); i++){
					NodeLet inl = node.getInLets().get(i);
					String varName = JavaCodeGenUtil.getVarName(inl.getDataName());
					if (i==0){//a
						inputVarName = varName;
					}else{//a.join(b)
						inputVarName = String.format("%s.union(%s)", inputVarName, varName);
					}
				}
				NodeLet inlet = node.getInLets().get(0);
				Data inData = flow.getDataDef(inlet.getDataName());
				DataType inputDataType = inData.getRecordType();
				String methodName = SparkUtil.getCmdSparkProcessMethod(inputDataType, outputDataType);
				if (methodName==null){
					logger.error(String.format("input:%s, output:%s pair on action:%s not supported.", inputDataType, outputDataType, anode.getName()));
				}
				String inputFormatName = InputFormatType.class.getName() + "." + inData.getDataFormat().toString();
				if (outputVarName!=null){
					sb.append(String.format("%s=%s.%s(%s,jsc,%s,spark);\n", outputVarName, cmdVarName, methodName, inputVarName, inputFormatName));
				}else{
					sb.append(String.format("%s.%s(%s,jsc,%s,spark);\n", cmdVarName, methodName, inputVarName, inputFormatName));
				}
				
				if (multiOutput){
					//call cache
					sb.append(String.format("%s.cache();\n", outputVarName));
					//filter output
					for (NodeLet outlet: node.getOutlets()){
						//data1 = SparkUtil.filterPairRDD(sftpOutput, "data1");
						String outletName = outlet.getName();
						String dataVarName = JavaCodeGenUtil.getVarName(outlet.getDataName());
						if (outletName.equals(dataVarName)){
							sb.append(String.format("%s=etl.util.SparkUtil.filterPairRDD(%s,\"%s\");\n", outletName, outputVarName, outletName));
						}else{
							logger.error(String.format("for split, the outlet name must same as the dataset name. now:%s!=%s", outletName, dataVarName));
						}
					}
				}else{//for single output
					if (node.getOutlets().size()>0){
						String dn = node.getOutlets().get(0).getDataName();
						int nusing = flow.getUsingDatasetNum(dn);
						if (nusing>1){
							//call cache
							//sb.append(String.format("%s.cache();\n", outputVarName));
						}
					}else{
						logger.warn(String.format("no outlet for node %s", node.getName()));
					}
				}
			}
		}
		//call some action on the last ds to make sure all transformation are exected
		List<String> lastData = flow.getLastDataSets();
		for (String ldn:lastData){
			String varName = JavaCodeGenUtil.getVarName(ldn);
			sb.append(String.format("logger.info(\"%s:\" + %s.collect());\n", varName, varName));
		}
		sb.append("jsc.close();\n");
		sb.append("return retInfo;");
		sb.append("}\n");
		//end class
		sb.append(String.format("}\n"));
		Files.write(file, sb.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
	}
}
