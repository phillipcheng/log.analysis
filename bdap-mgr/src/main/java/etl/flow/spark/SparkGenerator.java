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
import etl.engine.DataType;
import etl.engine.ETLCmd;
import etl.flow.ActionNode;
import etl.flow.Data;
import etl.flow.Flow;
import etl.flow.Node;
import etl.flow.NodeLet;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowMgr;
import etl.spark.SparkUtil;

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
		sb.append("import org.apache.spark.api.java.JavaPairRDD;\n");
		sb.append("import org.apache.spark.api.java.JavaRDD;\n");
		sb.append("import org.apache.spark.api.java.JavaSparkContext;\n");
		//begin class
		sb.append(String.format("public class %s extends %s implements %s {\n", className, ETLCmd.class.getName(), Serializable.class.getName()));
		sb.append(String.format("public static final Logger logger = Logger.getLogger(%s.class);\n", className));
		//gen constructor
		sb.append(String.format("public %s(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, etl.engine.ProcessMode pm){\n", className));
		sb.append("init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);}\n");
		//gen sgProcess
		sb.append("public List<String> sgProcess() {\n");
		sb.append("List<String> retInfo = new ArrayList<String>();\n");
		sb.append(String.format("SparkConf conf = new SparkConf().setAppName(getWfName());\n"));
		sb.append("JavaSparkContext jsc = new JavaSparkContext(conf);\n");
		//gen cmds
		List<Node> nodes = flow.getTopoOrder();
		List<Data> datas = flow.getData();
		for (Data data:datas){
			String varName = JavaCodeGenUtil.getVarName(data.getName());
			String varType;
			if (data.getRecordType().equals(DataType.Path) ||data.getRecordType().equals(DataType.Value)){
				varType = "JavaRDD<String>";
			}else if (data.getRecordType().equals(DataType.KeyPath) ||data.getRecordType().equals(DataType.KeyValue)){
				varType = "JavaPairRDD<String,String>";
			}else{
				logger.error(String.format("unsupported dataType for data %s", data));
				return;
			}
			String initValue="null";
			if (!data.isInstance()){
				if (data.getRecordType().equals(DataType.Path) ||data.getRecordType().equals(DataType.Value)){
					//JavaRDD<String> sftpMap= SparkUtil.fromFile(this.getDefaultFs() + "/flow1/sftpcfg/test1.sftp.map.properties", jsc);
					initValue = String.format("etl.spark.SparkUtil.fromFile(this.getDefaultFs() + \"%s\", jsc)", data.getLocation());
				}else if (data.getRecordType().equals(DataType.KeyPath) ||data.getRecordType().equals(DataType.KeyValue)){
					if (data.getBaseOutput()!=null){
						initValue = String.format("etl.spark.SparkUtil.fromFileKeyValue(this.getDefaultFs() + \"%s\", jsc, getHadoopConf(), \"%s\")", 
							data.getLocation(), data.getBaseOutput());
					}else{
						initValue = String.format("etl.spark.SparkUtil.fromFileKeyValue(this.getDefaultFs() + \"%s\", jsc, getHadoopConf(), null)", 
								data.getLocation());
					}
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
				sb.append(String.format("%s %s=new %s(getWfName(), getWfid(), \"%s\", super.getDefaultFs(), null);\n", 
						cmdClass, cmdVarName, cmdClass, propertyFileName));
				String outputVarName =null;
				boolean needFilter=false;
				DataType outputDataType = null;
				if (node.getOutlets().size()>1){//multiple outlet node, auto-generate an out data variable for filtering
					outputVarName = anode.getName() + "Output";
					needFilter=true;
					outputDataType = DataType.KeyValue;
					//additional output variable declaration
					sb.append(String.format("%s %s=%s;\n", "JavaPairRDD<String,String>", outputVarName, "null"));
				}else if (node.getOutlets().size()==1){
					NodeLet outlet = node.getOutlets().get(0);
					Data outData = flow.getDataDef(outlet.getDataName());
					outputVarName = JavaCodeGenUtil.getVarName(outlet.getDataName());
					outputDataType = outData.getRecordType();
				}else{//no output
					outputDataType = DataType.KeyValue;//default
				}
				//cmd execution line
				//data1trans = d1csvTransformCmd.sparkProcessFilesToKV(data1, jsc, TextInputFormat.class);
				String inputFormat=null;
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
				inputFormat = FlowDeployer.getInputFormat(inData.getDataFormat(), inData.getRecordType());
				if (outputVarName!=null){
					sb.append(String.format("%s=%s.%s(%s,jsc,%s.class);\n", outputVarName, cmdVarName, methodName, inputVarName, inputFormat));
				}else{
					sb.append(String.format("%s.%s(%s,jsc,%s.class);\n", cmdVarName, methodName, inputVarName, inputFormat));
				}
				if (needFilter){//filter output
					for (NodeLet outlet: node.getOutlets()){
						//data1 = SparkUtil.filterPairRDD(sftpOutput, "data1");
						String outletName = outlet.getName();
						String dataVarName = JavaCodeGenUtil.getVarName(outlet.getDataName());
						if (outletName.equals(dataVarName)){
							sb.append(String.format("%s=etl.spark.SparkUtil.filterPairRDD(%s,\"%s\");\n", outletName, outputVarName, outletName));
						}else{
							logger.error(String.format("for split, the outlet name must same as the dataset name. now:%s!=%s", outletName, dataVarName));
						}
					}
				}
			}
		}
		sb.append("jsc.close();\n");
		sb.append("return retInfo;");
		sb.append("}\n");
		//end class
		sb.append(String.format("}\n"));
		Files.write(file, sb.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
	}
}
