package etl.flow.oozie;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.EngineConf;
import bdap.util.PropertiesUtil;
import bdap.util.XmlUtil;
import etl.flow.Flow;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.oozie.coord.COORDINATORAPP;
import etl.flow.oozie.wf.WORKFLOWAPP;

public class OozieFlowMgr extends FlowMgr{
	
	public static final Logger logger = LogManager.getLogger(OozieFlowMgr.class);

	@Override
	public boolean deploy(String refRoot, String projectName, Flow flow, FlowServerConf fsconf, List<String> thirdPartyJars, EngineConf ec) {
		OozieConf oc = (OozieConf)fsconf;
		String projectDir;
		if (refRoot.endsWith(File.separator)){
			projectDir = String.format("%s%s", refRoot, projectName);
		}else{
			projectDir = String.format("%s%s%s", refRoot, File.separator, projectName);
		}
		Path path = Paths.get(projectDir);
		try {
			FileUtils.deleteDirectory(new File(projectDir));
			Files.createDirectories(path);
		} catch (IOException e) {
			logger.error("", e);
		}
		String wfFile = String.format("%s%s%s_workflow.xml", projectDir, File.separator, flow.getName());
		{//gen flow_workflow.xml and flow_actionX.properties
			WORKFLOWAPP wfa = OozieGenerator.genWfXml(flow);
			XmlUtil.marshal(wfa, "WORKFLOW-APP", wfFile);
			//gen flow_action.properties
			genProperties(flow, projectDir);
		}
		String wfJobProperties = String.format("%s%s%s_wf.job.properties", projectDir, File.separator, flow.getName());
		{//gen flow_job.properties
			Map<String, String> propertyMap = new HashMap<String, String>();
			propertyMap.put(OozieConf.key_jobTracker,oc.getJobTracker());
			propertyMap.put(OozieConf.key_nameNode, oc.getNameNode());
			propertyMap.put(OozieConf.key_queueName, oc.getQueueName());
			propertyMap.put(OozieConf.key_oozieLibPath, oc.getOozieLibPath());
			propertyMap.put(OozieConf.key_oozieWfAppPath, 
					String.format("${nameNode}/user/${user.name}/%s/%s.workflow.xml", projectName, flow.getName()));
			PropertiesUtil.writePropertyFile(wfJobProperties, propertyMap);
		}
		String coordFile = String.format("%s%s%s_coordinate.xml", projectDir, File.separator, flow.getName());
		{//gen flow_coordinate.xml
			COORDINATORAPP coord = OozieGenerator.genCoordXml(flow);
			XmlUtil.marshal(coord, "COORDINATOR-APP", coordFile);
		}
		String coordinateJobProperties = String.format("%s%s%s_coordinate.job.properties", projectDir, File.separator, flow.getName());
		{//gen flow_coordinate.job.properties
			Map<String, String> propertyMap = new HashMap<String, String>();
			propertyMap.put(OozieConf.key_jobTracker,oc.getJobTracker());
			propertyMap.put(OozieConf.key_nameNode, oc.getNameNode());
			propertyMap.put(OozieConf.key_queueName, oc.getQueueName());
			propertyMap.put(OozieConf.key_oozieLibPath, oc.getOozieLibPath());
			propertyMap.put(OozieConf.key_oozieWfAppPath, 
					String.format("${nameNode}/user/${user.name}/%s/%s.workflow.xml", projectName, flow.getName()));
			propertyMap.put(OozieConf.key_oozieCoordinateAppPath, 
					String.format("${nameNode}/user/${user.name}/%s/%s_coordinator.xml", projectName, flow.getName()));
			PropertiesUtil.writePropertyFile(coordinateJobProperties, propertyMap);
		}
		String etlengineProperties = String.format("%s%s%s", projectDir, File.separator, EngineConf.file_name);
		{//gen etlengine.properties
			ec.writeFile(etlengineProperties);
		}
		{//deploy:
			//copy workflow xml, coordinate xml to hdfs:/project/
			//copy thirdparty.jars to hdfs:/project/lib/
			//copy action.properties, etlengine.properties to hdfs:/project/lib/
			
		}
		return true;
	}

	@Override
	public boolean execute(String projectName, String flowName, String wfid, String startNode) {
		if (wfid==null){
			//start the workflow
		}else{
			
		}
		return false;
	}

}
