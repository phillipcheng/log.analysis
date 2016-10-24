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
import org.apache.hadoop.fs.FileSystem;

import bdap.util.EngineConf;
import bdap.util.HdfsUtil;
import bdap.util.PropertiesUtil;
import bdap.util.XmlUtil;
import etl.flow.Flow;
import etl.flow.mgr.FileType;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.coord.COORDINATORAPP;
import etl.flow.oozie.wf.WORKFLOWAPP;

public class OozieFlowMgr extends FlowMgr{
	
	public static final Logger logger = LogManager.getLogger(OozieFlowMgr.class);
	
	private InMemFile genWfXml(Flow flow, String startNode, boolean useInstanceId){
		String wfFileName = null;
		if (startNode==null){
			wfFileName = String.format("%s_workflow.xml", flow.getName());
		}else{
			wfFileName = String.format("%s_%s_workflow.xml", flow.getName(), startNode);
		}
		//gen flow_workflow.xml and flow_actionX.properties
		WORKFLOWAPP wfa = OozieGenerator.genWfXml(flow, startNode, useInstanceId);
		byte[] bytes = XmlUtil.marshalToBytes(wfa, "WORKFLOW-APP");
		return new InMemFile(FileType.oozieWfXml, wfFileName, bytes);
	}
	
	private InMemFile genEnginePropertyFile(){
		//TODO
		String etlengineProperties = String.format("%s", EngineConf.file_name);
		//ec.writeFile(etlengineProperties);
		return null;
	}

	@Override
	public String execute(String projectName, Flow flow, FlowServerConf fsconf, EngineConf ec, String startNode, String instanceId) {
		String newInstanceId = null;
		OozieConf oc = (OozieConf)fsconf;
		
		//generate wf.xml
		boolean useInstanceId = instanceId==null? false:true;
		InMemFile wfXml = genWfXml(flow, startNode, useInstanceId);
		//gen action.properties
		List<InMemFile> actionPropertyFiles = super.genProperties(flow);
		//gen etlengine.properties
		
		//deploy to the server
		FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
		//start the job
		return newInstanceId;
	}
	
	@Override
	public boolean deploy(String projectName, Flow flow, FlowServerConf fsconf, EngineConf ec) {
		OozieConf oc = (OozieConf)fsconf;
		String wfJobProperties = String.format("%s_wf.job.properties", flow.getName());
		{//gen flow_job.properties
			Map<String, String> propertyMap = new HashMap<String, String>();
			propertyMap.put(OozieConf.key_jobTracker,oc.getJobTracker());
			propertyMap.put(OozieConf.key_nameNode, oc.getNameNode());
			propertyMap.put(OozieConf.key_queueName, oc.getQueueName());
			propertyMap.put(OozieConf.key_oozieLibPath, oc.getOozieLibPath());
			propertyMap.put(OozieConf.key_oozieWfAppPath, 
					String.format("${nameNode}/user/${user.name}/%s/%s.workflow.xml", projectName, flow.getName()));
			//PropertiesUtil.writePropertyFile(wfJobProperties, propertyMap);
		}
		String coordFile = String.format("%s_coordinate.xml", flow.getName());
		{//gen flow_coordinate.xml
			COORDINATORAPP coord = OozieGenerator.genCoordXml(flow);
			XmlUtil.marshal(coord, "COORDINATOR-APP", coordFile);
		}
		String coordinateJobProperties = String.format("%s_coordinate.job.properties", flow.getName());
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
			//PropertiesUtil.writePropertyFile(coordinateJobProperties, propertyMap);
		}
		String etlengineProperties = String.format("%s",EngineConf.file_name);
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
	public boolean uploadJars(InMemFile[] files, FlowServerConf fsconf, EngineConf ec) {
		// TODO Auto-generated method stub
		return false;
	}

}
