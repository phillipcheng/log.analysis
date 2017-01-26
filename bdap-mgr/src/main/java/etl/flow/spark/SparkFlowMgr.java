package etl.flow.spark;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.FileType;
import bdap.util.JsonUtil;
import bdap.util.SystemUtil;
import bdap.util.XmlUtil;
import bdap.util.ZipUtil;
import dv.util.RequestUtil;
import etl.flow.Flow;
import etl.flow.deploy.DeployMethod;
import etl.flow.deploy.EngineType;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;

public class SparkFlowMgr extends OozieFlowMgr {
	public static final Logger logger = LogManager.getLogger(FlowMgr.class);

	public static final String spark_submit_sh="submitspark.sh";
	public static final String replace_wfname="[[workflowName]]";
	public static final String replace_thirdpartyjars="[[thirdpartyjars]]";
	public static final String replace_sparkhome="[[sparkhome]]";
	public static final String replace_defaultfs="[[defaultfs]]";
	public static final String replace_sparkhistoryserver="[[sparkhistoryserver]]";

	private static final String key_engineJars = "engineJars";
	
	public SparkFlowMgr() {
		super();
	}

	public SparkFlowMgr(DeployMethod deployMethod) {
		super(deployMethod);
	}

	public SparkFlowMgr(FileSystem fs) {
		super(fs);
	}

	@Override
	public boolean deployFlow(String prjName, Flow flow, String[] jars, String[] propFiles, FlowDeployer fd) throws Exception{
		boolean manual=false;
		String flowName = flow.getName();
		SparkServerConf ssc = fd.getSparkServerConf();
		String prjFolder = String.format("%s/%s", ssc.getTmpFolder(), prjName);
		Files.createDirectories(Paths.get(prjFolder));
		//clean up the prjFolder
		if (!manual){
			Files.walk(Paths.get(prjFolder), FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
		}
		String targetDir = String.format("%s/%s/%s", ssc.getTmpFolder(), prjName, ssc.getTargetFolder());
		Files.createDirectories(Paths.get(targetDir));
		//generate the driver java file
		String srcRootDir = String.format("%s/%s/%s", ssc.getTmpFolder(), prjName, ssc.getSrcFolder());
		if (!Files.exists(Paths.get(srcRootDir))) Files.createDirectories(Paths.get(srcRootDir));
		if (!manual){
			SparkGenerator.genDriverJava(prjName, flow, srcRootDir, ssc);
		}
		//compile the file
		String classesRootDir = String.format("%s/%s/%s", ssc.getTmpFolder(), prjName, ssc.getClassesFolder());
		if (!Files.exists(Paths.get(classesRootDir))) Files.createDirectories(Paths.get(classesRootDir));
		String cpPath = String.format("%s/buildlib/*", fd.getPlatformLocalDist());
		if (jars != null)
			for (String jar: jars)
				cpPath = cpPath + File.pathSeparator + jar;
		String javacmd = String.format("%s/javac -cp \"%s\" -d %s %s/%s/%s.java", ssc.getJdkBin(), cpPath, 
				classesRootDir, srcRootDir, prjName, flowName);
		logger.info(javacmd);
		String output = SystemUtil.execCmd(javacmd);
		logger.info(output);
		//generate action properties files
		List<InMemFile> localImFiles = new ArrayList<InMemFile>();
		localImFiles.add(super.genEnginePropertyFile(fd.getEngineConfig()));
		//generate etlengine.properties
		localImFiles.addAll(super.genProperties(flow, EngineType.spark));
		for (InMemFile im: localImFiles){
			Path path = Paths.get(String.format("%s/%s", classesRootDir, im.getFileName()));
			Files.write(path, im.getContent());
		}
		if (propFiles!=null){
			for (String propFile: propFiles){
				Path propSrcPath = Paths.get(propFile);
				String propFileName = propSrcPath.getFileName().toString();
				Path propDestPath = Paths.get(String.format("%s/%s", classesRootDir, propFileName));
				Files.write(propDestPath, Files.readAllBytes(propSrcPath));
			}
		}
		//jar the file
		String jarFilePath = String.format("%s/%s.jar", targetDir, flowName);
		ZipUtil.makeJar(jarFilePath, classesRootDir);
		//deploy ${wfName}.jar
		List<InMemFile> remoteImFiles = new ArrayList<InMemFile>();
		remoteImFiles.add(new InMemFile(FileType.thirdpartyJar, String.format("%s.jar", flowName), Files.readAllBytes(Paths.get(jarFilePath))));
		//add thirdparty jar
		String hdfsPrjFolder = fd.getProjectHdfsDir(prjName);
		if (!hdfsPrjFolder.endsWith("/")){
			hdfsPrjFolder +="/";
		}
		String localPrjFolder = fd.getProjectLocalDir(prjName);
		String localFlowFolder = String.format("%s/%s", localPrjFolder, flowName);
		List<InMemFile> jarDU = FlowDeployer.getJarDU(localFlowFolder, jars);
		remoteImFiles.addAll(jarDU);
		//generate the workflow.xml file for oozie
		//get the sparkcmd_workflow.xml template
		String sparkWfTemplatePath = String.format("%s/cfg/sparkcmd_workflow.xml", fd.getPlatformLocalDist());
		String sparkWfTemplate = new String(Files.readAllBytes(Paths.get(sparkWfTemplatePath)), StandardCharsets.UTF_8);
		String wfName = String.format("%s_%s", prjName, flowName);
		StringBuffer thirdPartyJarsSb = new StringBuffer();
		for (InMemFile jar:jarDU){
			String strJar=String.format("%s%s%s/lib/%s", fd.getDefaultFS(), hdfsPrjFolder, flowName, jar.getFileName());
			thirdPartyJarsSb.append(",").append(strJar);
		}
		
		if (fd.existsDir(String.format("%s%s%s/lib", fd.getDefaultFS(), hdfsPrjFolder, flowName))) {
			/* Add 3rd party jars if the lib directory exists */
			List<String> libfiles = fd.listFiles(String.format("%s%s%s/lib", fd.getDefaultFS(), hdfsPrjFolder, flowName));
			if (libfiles != null) {
				for (String f: libfiles) {
					if (f.endsWith(".jar") && (!f.startsWith(flowName))) {
						String strJar=String.format("%s%s%s/lib/%s", fd.getDefaultFS(), hdfsPrjFolder, flowName, f);
						if (thirdPartyJarsSb.indexOf(strJar) == -1)
							thirdPartyJarsSb.append(",").append(strJar);
					}
				}
			}
		}
		
		String sparkWf = sparkWfTemplate.replace(replace_wfname, wfName).replace(replace_thirdpartyjars, thirdPartyJarsSb.toString())
				.replace(replace_defaultfs, fd.getDefaultFS()).replace(replace_sparkhome, ssc.getSparkHome()).replace(replace_sparkhistoryserver, ssc.getSparkHistoryServer());
		remoteImFiles.add(new InMemFile(FileType.oozieWfXml, String.format("%s_workflow.xml", flowName), sparkWf.getBytes(StandardCharsets.UTF_8)));
		//copy the shell to lib
		String submitSparkShellPath = String.format("%s/cfg/%s", fd.getPlatformLocalDist(), spark_submit_sh);
		String submitSparkTemplate = new String(Files.readAllBytes(Paths.get(submitSparkShellPath)), StandardCharsets.UTF_8);
		String submitSpark = submitSparkTemplate.replace(replace_sparkhome, ssc.getSparkHome());
		remoteImFiles.add(new InMemFile(FileType.shell, spark_submit_sh, submitSpark.getBytes(StandardCharsets.UTF_8)));
		uploadFiles(hdfsPrjFolder, flowName, remoteImFiles, fd);
		return false;
	}

	/*
	oozie.libpath=${nameNode}/bdap-VVERSIONN/lib/
	oozie.wf.application.path=${nameNode}/bdap-VVERSIONN/cfg/sparkcmd_workflow.xml
	yarn_historyserver=
	 */
	private bdap.xml.config.Configuration getWfConf(OozieConf oc, String prjName, String flowName, FlowDeployer fd){
		String hdfsPrjFolder = fd.getProjectHdfsDir(prjName);
		bdap.xml.config.Configuration bodyConf = new bdap.xml.config.Configuration();
		{
			bdap.xml.config.Configuration.Property oozieLibPath = new bdap.xml.config.Configuration.Property();
			oozieLibPath.setName(OozieConf.key_oozieLibPath);
			oozieLibPath.setValue(oc.getOozieLibPath());
			bodyConf.getProperty().add(oozieLibPath);
		}{
			//then the shell script has to be at ./lib/
			bdap.xml.config.Configuration.Property propWfAppPath = new bdap.xml.config.Configuration.Property();
			propWfAppPath.setName(OozieConf.key_oozieWfAppPath);
			String dir = getDir(FileType.oozieWfXml, hdfsPrjFolder, flowName, oc);
			propWfAppPath.setValue(String.format("%s%s_workflow.xml", dir, flowName));
			bodyConf.getProperty().add(propWfAppPath);
		}{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName(OozieConf.key_yarn_historyserver);
			property.setValue(oc.getYarnHistoryServer());
			bodyConf.getProperty().add(property);
		}{
			String fqClassCmdName=SparkGenerator.getFQClassName(prjName, flowName);
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName(OozieConf.key_cmdClassName);
			property.setValue(fqClassCmdName);
			bodyConf.getProperty().add(property);
		}{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName(key_engineJars);
			List<String> libfiles = fd.listFiles(String.format("%s%s/lib", fd.getDefaultFS(), fd.getPlatformRemoteDist()));
			StringBuilder engineJarsSb = new StringBuilder();
			if (libfiles != null) {
				int i = 0;
				for (String f: libfiles) {
					if (f.endsWith(".jar") && (!f.startsWith("bdap.engine-"))) {
						String strJar=String.format("%s%s/lib/%s", fd.getDefaultFS(), fd.getPlatformRemoteDist(), f);
						if (i == 0)
							engineJarsSb.append(strJar);
						else
							engineJarsSb.append(",").append(strJar);
						i ++;
					}
				}
			}
			property.setValue(engineJarsSb.toString());
			bodyConf.getProperty().add(property);
		}
		return bodyConf;
	}
	
	@Override
	public String executeFlow(String prjName, String flowName, FlowDeployer fd) {
		OozieConf oozieServerConf = fd.getOozieServerConf();
		String jobSumbitUrl=String.format("http://%s:%d/oozie/v1/jobs", oozieServerConf.getOozieServerIp(), oozieServerConf.getOozieServerPort());
		Map<String, String> queryParamMap = new HashMap<String, String>();
		queryParamMap.put(OozieConf.key_oozie_action, OozieConf.value_action_start);
		bdap.xml.config.Configuration commonConf = getCommonConf(fd, prjName, flowName);
		bdap.xml.config.Configuration wfConf = getWfConf(oozieServerConf, prjName, flowName, fd);
		commonConf.getProperty().addAll(wfConf.getProperty());
		String body = XmlUtil.marshalToString(commonConf, "configuration");
		String result = RequestUtil.post(jobSumbitUrl, null, 0, queryParamMap, null, body);
		logger.info(String.format("post result:%s", result));
		Map<String, String> vm = JsonUtil.fromJsonString(result, HashMap.class);
		if (vm!=null){
			return vm.get("id");
		}else{
			return null;
		}
	}
}
