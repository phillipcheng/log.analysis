package etl.flow.spark;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.EngineConf;
import bdap.util.FileType;
import bdap.util.JsonUtil;
import bdap.util.SystemUtil;
import bdap.util.XmlUtil;
import bdap.util.ZipUtil;
import dv.util.RequestUtil;
import etl.flow.CoordConf;
import etl.flow.Flow;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.mgr.InMemFile;
import etl.flow.mgr.NodeInfo;
import etl.flow.oozie.OozieConf;

public class SparkFlowMgr extends FlowMgr{
	public static final Logger logger = LogManager.getLogger(FlowMgr.class);

	@Override
	public boolean deployFlowFromJson(String prjName, Flow flow, FlowDeployer fd) throws Exception{
		SparkServerConf ssc = fd.getSparkServerConf();
		String targetDir = String.format("%s/%s/%s", ssc.getTmpFolder(), prjName, ssc.getTargetFolder());
		if (!Files.exists(Paths.get(targetDir))) Files.createDirectories(Paths.get(targetDir));
		//generate the driver java file
		String srcRootDir = String.format("%s/%s/%s", ssc.getTmpFolder(), prjName, ssc.getSrcFolder());
		if (!Files.exists(Paths.get(srcRootDir))) Files.createDirectories(Paths.get(srcRootDir));
		SparkGenerator.genDriverJava(prjName, flow, srcRootDir, ssc);
		//compile the file
		String classesRootDir = String.format("%s/%s/%s", ssc.getTmpFolder(), prjName, ssc.getClassesFolder());
		if (!Files.exists(Paths.get(classesRootDir))) Files.createDirectories(Paths.get(classesRootDir));
		String cpPath = String.format("%s/buildlib/*", fd.getPlatformLocalDist());
		String javacmd = String.format("%s/javac -cp \"%s\" -d %s %s/%s/%s.java", ssc.getJdkBin(), cpPath, 
				classesRootDir, srcRootDir, prjName, flow.getName());
		logger.info(javacmd);
		String output = SystemUtil.execCmd(javacmd);
		logger.info(output);
		//generate action properties files
		List<InMemFile> imfiles = new ArrayList<InMemFile>();
		imfiles.add(super.genEnginePropertyFile(fd.getEngineConfig()));
		//generate etlengine.properties
		imfiles.addAll(super.genProperties(flow));
		for (InMemFile im: imfiles){
			Path path = Paths.get(String.format("%s/%s", classesRootDir, im.getFileName()));
			Files.write(path, im.getContent());
		}
		//jar the file
		String jarFilePath = String.format("%s/%s.jar", targetDir, flow.getName());
		ZipUtil.makeJar(jarFilePath, classesRootDir);
		//deploy ${wfName}.jar
		List<InMemFile> imFiles = new ArrayList<InMemFile>();
		imFiles.add(new InMemFile(FileType.thirdpartyJar, String.format("%s.jar", flow.getName()), Files.readAllBytes(Paths.get(jarFilePath))));
		String projectDir = fd.getProjectHdfsDir(prjName);
		uploadFiles(projectDir, flow.getName(), imFiles.toArray(new InMemFile[]{}), ssc.getOozieServerConf(), fd.getFs());
		return false;
	}

	/*
	oozie.libpath=${nameNode}/bdap-VVERSIONN/lib/
	oozie.wf.application.path=${nameNode}/bdap-VVERSIONN/cfg/sparkcmd_workflow.xml
	yarn_historyserver=
	 */
	private bdap.xml.config.Configuration getWfConf(OozieConf oc, String prjName, String flowName, FlowDeployer fd){
		bdap.xml.config.Configuration bodyConf = new bdap.xml.config.Configuration();
		{
			bdap.xml.config.Configuration.Property oozieLibPath = new bdap.xml.config.Configuration.Property();
			oozieLibPath.setName(OozieConf.key_oozieLibPath);
			oozieLibPath.setValue(oc.getOozieLibPath());
			bodyConf.getProperty().add(oozieLibPath);
		}{
			bdap.xml.config.Configuration.Property propWfAppPath = new bdap.xml.config.Configuration.Property();
			propWfAppPath.setName(OozieConf.key_oozieWfAppPath);
			propWfAppPath.setValue(String.format("%s/cfg/%s", fd.getPlatformRemoteDist(), FlowDeployer.spark_wfxml));
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
		}
		return bodyConf;
	}
	
	@Override
	public String executeFlow(String prjName, String flowName, FlowDeployer fd) {
		SparkServerConf sparkServerConf = fd.getSparkServerConf();
		OozieConf oozieServerConf = sparkServerConf.getOozieServerConf();
		String jobSumbitUrl=String.format("http://%s:%d/oozie/v1/jobs", oozieServerConf.getOozieServerIp(), oozieServerConf.getOozieServerPort());
		Map<String, String> queryParamMap = new HashMap<String, String>();
		queryParamMap.put(OozieConf.key_oozie_action, OozieConf.value_action_start);
		bdap.xml.config.Configuration commonConf = getCommonConf(oozieServerConf, prjName, flowName);
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

	@Override
	public String executeCoordinator(String prjName, String flowName, FlowDeployer fd, CoordConf cc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFlowLog(String projectName, FlowServerConf fsconf, String instanceId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InMemFile[] getNodeLog(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NodeInfo getNodeInfo(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void executeAction(String projectName, Flow flow, List<InMemFile> imFiles, FlowServerConf fsconf,
			EngineConf ec, String actionNode, String instanceId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public FlowInfo getFlowInfo(String projectName, FlowServerConf fsconf, String instanceId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] listNodeInputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId,
			String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] listNodeOutputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId,
			String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InMemFile getDFSFile(EngineConf ec, String filePath) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InMemFile getDFSFile(EngineConf ec, String filePath, int maxFileSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean putDFSFile(EngineConf ec, String filePath, InMemFile file) {
		// TODO Auto-generated method stub
		return false;
	}

}
