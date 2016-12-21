package etl.flow.mgr;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.EngineConf;
import bdap.util.FileType;
import bdap.util.JsonUtil;
import bdap.util.PropertiesUtil;
import bdap.util.Util;
import etl.flow.ActionNode;
import etl.flow.CoordConf;
import etl.flow.Flow;
import etl.flow.Node;
import etl.flow.deploy.FlowDeployer;
import etl.flow.oozie.OozieConf;

public abstract class FlowMgr {
	public static final Logger logger = LogManager.getLogger(FlowMgr.class);
	
	public static String getPropFileName(String nodeName){
		return String.format("action_%s.properties", nodeName);
	}
	
	//generate the properties files for all the cmd to initiate
	public List<InMemFile> genProperties(Flow flow){
		List<InMemFile> propertyFiles = new ArrayList<InMemFile>();
		for (Node n: flow.getNodes()){
			if (n instanceof ActionNode){
				ActionNode an = (ActionNode) n;
				String propFileName = getPropFileName(an.getName());
				byte[] bytes = PropertiesUtil.getPropertyFileContent(an.getUserProperties());
				propertyFiles.add(new InMemFile(FileType.actionProperty, propFileName, bytes));
			}
		}
		return propertyFiles;
	}
	
	public InMemFile genEnginePropertyFile(EngineConf ec){
		return new InMemFile(FileType.engineProperty, EngineConf.file_name, ec.getContent());
	}
	
	//projectDir is hdfsDir
	public String getDir(FileType ft, String projectDir, String flowName, OozieConf oc){
		String nameNodePath = oc.getNameNode();
		if (nameNodePath.endsWith("/")){
			nameNodePath = nameNodePath.substring(0, nameNodePath.length() - 1);
		}
		String hdfsDir = projectDir;
		if (hdfsDir.endsWith("/")){
			hdfsDir = hdfsDir.substring(0, hdfsDir.length() - 1);
		}
		if (hdfsDir.startsWith("/")){
			hdfsDir = hdfsDir.substring(1);
		}
		if (ft == FileType.actionProperty || ft == FileType.engineProperty || 
				ft==FileType.thirdpartyJar || ft == FileType.ftmappingFile || 
				ft==FileType.log4j || ft == FileType.logicSchema){
			return String.format("%s/%s/%s/lib/", nameNodePath, hdfsDir, flowName);
		}else if (ft == FileType.oozieWfXml || ft == FileType.oozieCoordXml){
			return String.format("%s/%s/%s/", nameNodePath, hdfsDir, flowName);
		}else{
			logger.error("file type not supported:%s", ft);
			return null;
		}
	}
	
	public void uploadFiles(String projectDir, String flowName, InMemFile[] files, OozieConf oc, FlowDeployer fd) {
		//deploy to the server
		for (InMemFile im:files){
			String dir = getDir(im.getFileType(), projectDir, flowName, oc);
			String path = String.format("%s%s", dir, im.getFileName());
			logger.info(String.format("copy to %s", path));
			fd.deploy(path, im.getContent());
		}
	}
	
	//return common properties: nameNode, jobTracker, queue, username, useSystem
	public bdap.xml.config.Configuration getCommonConf(OozieConf oc, String prjName, String wfName){
		bdap.xml.config.Configuration bodyConf = new bdap.xml.config.Configuration();
		{
			bdap.xml.config.Configuration.Property propNameNode = new bdap.xml.config.Configuration.Property();
			propNameNode.setName(OozieConf.key_nameNode);
			propNameNode.setValue(oc.getNameNode());
			bodyConf.getProperty().add(propNameNode);
		}{
			bdap.xml.config.Configuration.Property propJobTracker = new bdap.xml.config.Configuration.Property();
			propJobTracker.setName(OozieConf.key_jobTracker);
			propJobTracker.setValue(oc.getJobTracker());
			bodyConf.getProperty().add(propJobTracker);
		}{
			bdap.xml.config.Configuration.Property queueName = new bdap.xml.config.Configuration.Property();
			queueName.setName(OozieConf.key_queueName);
			queueName.setValue(oc.getQueueName());
			bodyConf.getProperty().add(queueName);
		}{
			bdap.xml.config.Configuration.Property propWfAppPath = new bdap.xml.config.Configuration.Property();
			propWfAppPath.setName(OozieConf.key_user_name);
			propWfAppPath.setValue(oc.getUserName());
			bodyConf.getProperty().add(propWfAppPath);
		}{
			bdap.xml.config.Configuration.Property oozieLibPath = new bdap.xml.config.Configuration.Property();
			oozieLibPath.setName(OozieConf.key_useSystemPath);
			oozieLibPath.setValue("true");
			bodyConf.getProperty().add(oozieLibPath);
		}{
			bdap.xml.config.Configuration.Property flowName = new bdap.xml.config.Configuration.Property();
			flowName.setName(OozieConf.key_flowName);
			flowName.setValue(wfName);
			bodyConf.getProperty().add(flowName);
		}{
			bdap.xml.config.Configuration.Property prjNameProp = new bdap.xml.config.Configuration.Property();
			prjNameProp.setName(OozieConf.key_prjName);
			prjNameProp.setValue(prjName);
			bodyConf.getProperty().add(prjNameProp);
		}
		return bodyConf;
	}
	
	public abstract boolean deployFlowFromJson(String prjName, Flow flow, FlowDeployer fd) throws Exception;
	public abstract String executeFlow(String prjName, String flowName, FlowDeployer fd) throws Exception;
	public abstract String executeCoordinator(String prjName, String flowName, FlowDeployer fd, CoordConf cc)  throws Exception;
	/**
	 * execute an action node of an existing workflow instance
	 * @param projectName
	 * @param flow
	 * @param imFiles
	 * @param fsconf
	 * @param ec
	 * @param actionNode
	 * @param instanceId
	 * @return
	 */
	public abstract void executeAction(String projectName, Flow flow, List<InMemFile> imFiles, FlowServerConf fsconf, 
			EngineConf ec, String actionNode, String instanceId);
	
	/**
	 * get the flow info of submitted instance
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @return flow info
	 */
	
	public abstract FlowInfo getFlowInfo(String projectName, FlowServerConf fsconf, String instanceId);
	
	/**
	 * get the flow log of submitted instance
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return log content
	 */
	public abstract String getFlowLog(String projectName, FlowServerConf fsconf, String instanceId);
	
	/**
	 * get the action node log of submitted instance
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return log content
	 */
	public abstract InMemFile[] getNodeLog(String projectName, FlowServerConf fsconf, String instanceId, String nodeName);
	
	/**
	 * get the action node info of submitted instance
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return node info
	 */
	public abstract NodeInfo getNodeInfo(String projectName, FlowServerConf fsconf, String instanceId, String nodeName);
	
	/**
	 * list the action node's input files
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return list of file paths
	 */
	public abstract String[] listNodeInputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId, String nodeName);
	
	/**
	 * list the action node's output files
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return list of file paths
	 */
	public abstract String[] listNodeOutputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId, String nodeName);
	
	/**
	 * get the distributed file
	 * @param fsconf
	 * @param filePath
	 * @return file content (Max file default size: 1MB)
	 */
	public abstract InMemFile getDFSFile(EngineConf ec, String filePath);
	

	/**
	 * get the distributed file
	 * @param fsconf
	 * @param filePath
	 * @param maxFileSize
	 * @return file content
	 */
	public abstract InMemFile getDFSFile(EngineConf ec, String filePath, int maxFileSize);

	/**
	 * put the distributed file
	 * @param ec
	 * @param filePath
	 * @param file
	 * @return true/false
	 */
	public abstract boolean putDFSFile(EngineConf ec, String filePath, InMemFile file);
	
	//generate json file from java construction
	public static void genFlowJson(String rootFolder, String projectName, Flow flow){
		String jsonFileName=String.format("%s/%s/%s/%s.json", rootFolder, projectName, flow.getName(), flow.getName());
		String jsonString = JsonUtil.toJsonString(flow);
		Util.writeFile(jsonFileName, jsonString);
	}
}
