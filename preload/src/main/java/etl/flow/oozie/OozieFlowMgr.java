package etl.flow.oozie;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.flow.Flow;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.oozie.wf.WORKFLOWAPP;
import etl.util.LocalPropertiesUtil;

public class OozieFlowMgr extends FlowMgr{
	
	public static final Logger logger = LogManager.getLogger(OozieFlowMgr.class);

	@Override
	public boolean deploy(String projectName, Flow flow, FlowServerConf fsconf) {
		OozieConf oc = (OozieConf)fsconf;
		String dir = projectName;
		Path path = Paths.get(dir);
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			logger.error("", e);
		}
		//gen flow_workflow.xml
		WORKFLOWAPP wfa = OozieGenerator.genWfXml(flow);
		String pdeflowFile = String.format("%s%s%s_workflow.xml", dir, File.separator, flow.getName());
		FlowJaxbUtil.marshal(wfa, pdeflowFile);
		//gen flow_action.properties
		genProperties(flow, dir);
		//gen flow_job.properties
		Map<String, String> propertyMap = new HashMap<String, String>();
		propertyMap.put(OozieConf.key_jobTracker,oc.getJobTracker());
		propertyMap.put(OozieConf.key_nameNode, oc.getNameNode());
		propertyMap.put(OozieConf.key_queueName, oc.getQueueName());
		propertyMap.put(OozieConf.key_oozieLibPath, oc.getOozieLibPath());
		propertyMap.put(OozieConf.key_oozieWfAppPath, 
				String.format("${nameNode}/user/${user.name}/%s/%s.workflow.xml", projectName, flow.getName()));
		LocalPropertiesUtil.writePropertyFile(String.format("%s%s%s_job.properties", dir, File.separator, flow.getName()), propertyMap);
		return true;
	}

	@Override
	public boolean execute(String flowName, String wfid, String startNode) {
		// TODO Auto-generated method stub
		return false;
	}

}
