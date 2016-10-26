package etl.flow.oozie;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import bdap.util.EngineConf;
import bdap.util.HdfsUtil;
import bdap.util.XmlUtil;
import dv.util.RequestUtil;
import etl.flow.Flow;
import etl.flow.mgr.FileType;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.coord.COORDINATORAPP;
import etl.flow.oozie.wf.WORKFLOWAPP;

public class OozieFlowMgr extends FlowMgr{
	
	public static final Logger logger = LogManager.getLogger(OozieFlowMgr.class);
	
	private String getDir(FileType ft, String projectName, OozieConf oc){
		if (ft == FileType.actionProperty || ft == FileType.engineProperty || 
				ft==FileType.thirdpartyJar || ft == FileType.ftmappingFile){
			return String.format("%s/user/%s/%s/lib/", oc.getNameNode(), oc.getUserName(), projectName);
		}else if (ft == FileType.oozieWfXml || ft == FileType.oozieCoordXml){
			return String.format("%s/user/%s/%s/", oc.getNameNode(), oc.getUserName(), projectName);
		}else{
			logger.error("file type not supported:%s", ft);
			return null;
		}
	}
	
	public String genWfXmlFile(Flow flow){
		return genWfXmlFile(flow, null, false);
	}
	
	public String genWfXmlFile(Flow flow, String startNode, boolean useInstanceId){
		WORKFLOWAPP wfa = OozieGenerator.genWfXml(flow, startNode, useInstanceId);
		String flowXml = XmlUtil.marshalToString(wfa, "workflow-app");
		return flowXml.replace("xmlns:ns5=", "xmlns=");
	}
	
	private InMemFile genWfXml(Flow flow, String startNode, boolean useInstanceId){
		String wfFileName = null;
		if (startNode==null){
			wfFileName = String.format("%s_workflow.xml", flow.getName());
		}else{
			wfFileName = String.format("%s_%s_workflow.xml", flow.getName(), startNode);
		}
		//gen flow_workflow.xml and flow_actionX.properties
		String flowXml = genWfXmlFile(flow, startNode, useInstanceId);
		return new InMemFile(FileType.oozieWfXml, wfFileName, flowXml.getBytes());
	}
	
	private InMemFile genCoordXml(Flow flow){
		String coordFile = String.format("%s_coordinate.xml", flow.getName());
		//gen flow_coordinate.xml
		COORDINATORAPP coord = OozieGenerator.genCoordXml(flow);
		byte[] content = XmlUtil.marshalToBytes(coord, "coordinate-app");
		return new InMemFile(FileType.oozieCoordXml, coordFile, content);
	}
	
	private bdap.xml.config.Configuration getBodyConf(OozieConf oc, String projectName, String flowName){
		bdap.xml.config.Configuration bodyConf = new bdap.xml.config.Configuration();
		{
			bdap.xml.config.Configuration.Property propUserName = new bdap.xml.config.Configuration.Property();
			propUserName.setName(OozieConf.key_user_name);
			propUserName.setValue(oc.getUserName());
			bodyConf.getProperty().add(propUserName);
		}{
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
			bdap.xml.config.Configuration.Property oozieLibPath = new bdap.xml.config.Configuration.Property();
			oozieLibPath.setName(OozieConf.key_oozieLibPath);
			oozieLibPath.setValue(oc.getOozieLibPath());
			bodyConf.getProperty().add(oozieLibPath);
		}{
			bdap.xml.config.Configuration.Property propWfAppPath = new bdap.xml.config.Configuration.Property();
			propWfAppPath.setName(OozieConf.key_oozieWfAppPath);
			propWfAppPath.setValue(String.format("%s/user/%s/%s/%s_workflow.xml", oc.getNameNode(), oc.getUserName(), projectName, flowName));
			bodyConf.getProperty().add(propWfAppPath);
		}
		return bodyConf;
	}

	@Override
	public String execute(String projectName, Flow flow, FlowServerConf fsconf, EngineConf ec, String startNode, String instanceId) {
		String newInstanceId = null;
		OozieConf oc = (OozieConf)fsconf;
		//generate wf.xml
		boolean useInstanceId = instanceId==null? false:true;
		List<InMemFile> imFiles = new ArrayList<InMemFile>();
		InMemFile wfXml = genWfXml(flow, startNode, useInstanceId);
		imFiles.add(wfXml);
		//gen action.properties
		List<InMemFile> actionPropertyFiles = super.genProperties(flow);
		imFiles.addAll(actionPropertyFiles);
		//gen etlengine.properties
		InMemFile enginePropertyFile = super.genEnginePropertyFile(ec);
		imFiles.add(enginePropertyFile);
		//deploy to the server
		FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
		try {
			fs.delete(new Path(String.format("/user/%s/%s", oc.getUserName(), projectName)), true);
		}catch(Exception e){
			logger.error("", e);
		}
		for (InMemFile im:imFiles){
			String dir = getDir(im.getFileType(), projectName, oc);
			String path = String.format("%s%s", dir, im.getFileName());
			HdfsUtil.writeDfsFile(fs, path, im.getContent());
		}
		//start the job
		String jobSumbitUrl=String.format("http://%s:%d/oozie/v1/jobs", oc.getOozieServerIp(), oc.getOozieServerPort());
		Map<String, String> queryParamMap = new HashMap<String, String>();
		queryParamMap.put(OozieConf.key_oozie_action, OozieConf.value_action_start);
		bdap.xml.config.Configuration bodyConf = getBodyConf(oc, projectName, flow.getName());
		String body = XmlUtil.marshalToString(bodyConf, "configuration");
		RequestUtil.post(jobSumbitUrl, null, 0, queryParamMap, null, body);
		return newInstanceId;
	}
	
	@Override
	public boolean deploy(String projectName, Flow flow, FlowServerConf fsconf, EngineConf ec) {
		OozieConf oc = (OozieConf)fsconf;
		List<InMemFile> imFiles = new ArrayList<InMemFile>();
		//gen wf xml
		InMemFile wfXml = genWfXml(flow, null, false);
		imFiles.add(wfXml);
		//gen coord xml
		InMemFile coordXml = genCoordXml(flow);
		imFiles.add(coordXml);
		//gen action.properties
		List<InMemFile> actionPropertyFiles = super.genProperties(flow);
		imFiles.addAll(actionPropertyFiles);
		//gen etlengine.properties
		InMemFile enginePropertyFile = super.genEnginePropertyFile(ec);
		imFiles.add(enginePropertyFile);
		//deploy to the server
		FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
		for (InMemFile im:imFiles){
			String dir = getDir(im.getFileType(), projectName, oc);
			String path = String.format("%s%s", dir, im.getFileName());
			HdfsUtil.writeDfsFile(fs, path, im.getContent());
		}
		//start the coordinator
		String jobSumbitUrl=String.format("http://%s:%d/oozie/v1/jobs", oc.getOozieServerIp(), oc.getOozieServerPort());
		Map<String, String> headMap = new HashMap<String, String>();
		bdap.xml.config.Configuration bodyConf = getBodyConf(oc, projectName, flow.getName());
		bdap.xml.config.Configuration.Property propWfAppPath = new bdap.xml.config.Configuration.Property();
		propWfAppPath.setName(OozieConf.key_oozieCoordinateAppPath);
		propWfAppPath.setValue(String.format("%s/user/%s/%s/%s_coordinator.xml", oc.getNameNode(), oc.getUserName(), projectName, flow.getName()));
		bodyConf.getProperty().add(propWfAppPath);
		String body = XmlUtil.marshalToString(bodyConf, "configuration");
		RequestUtil.post(jobSumbitUrl, headMap, body);
		return true;
	}

	@Override
	public boolean uploadJars(InMemFile[] files, FlowServerConf fsconf, EngineConf ec) {
		// TODO Auto-generated method stub
		return false;
	}

}
