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

public abstract class FlowMgr {
	public static final Logger logger = LogManager.getLogger(FlowMgr.class);
	
	//generate the properties files for all the cmd to initiate
	public List<InMemFile> genProperties(Flow flow){
		List<InMemFile> propertyFiles = new ArrayList<InMemFile>();
		for (Node n: flow.getNodes()){
			if (n instanceof ActionNode){
				ActionNode an = (ActionNode) n;
				String propFileName = String.format("action_%s.properties", an.getName());
				byte[] bytes = PropertiesUtil.getPropertyFileContent(an.getUserProperties());
				propertyFiles.add(new InMemFile(FileType.actionProperty, propFileName, bytes));
			}
		}
		return propertyFiles;
	}
	
	public InMemFile genEnginePropertyFile(EngineConf ec){
		return new InMemFile(FileType.engineProperty, EngineConf.file_name, ec.getContent());
	}
	
	public abstract boolean deployFlowFromJson(String projectName, Flow flow, FlowDeployer fd, FlowServerConf fsconf, EngineConf ec) throws Exception;
	public abstract String executeFlow(String projectDir, String flowName, FlowServerConf fsconf, EngineConf ec) throws Exception;
	public abstract String executeCoordinator(String projectDir, String flowName, FlowServerConf fsconf, EngineConf ec, CoordConf cc)  throws Exception;

	/**
	 * update helper jars, mapping.properties
	 * @param files
	 * @param fsconf
	 * @param ec
	 * @return
	 */
	public abstract void uploadFiles(String projectName, String flowName, InMemFile[] files, FlowServerConf fsconf, EngineConf ec);
	
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
	
	//generate json file from java construction
	public static void genFlowJson(String rootFolder, String projectName, Flow flow){
		String jsonFileName=String.format("%s/%s/%s/%s.json", rootFolder, projectName, flow.getName(), flow.getName());
		String jsonString = JsonUtil.toJsonString(flow);
		Util.writeFile(jsonFileName, jsonString);
	}
}
