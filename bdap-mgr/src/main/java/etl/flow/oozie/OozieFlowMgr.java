package etl.flow.oozie;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;
import bdap.util.EngineConf;
import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import bdap.util.XmlUtil;
import dv.util.RequestUtil;
import etl.flow.Flow;
import etl.flow.mgr.FileType;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.mgr.InMemFile;
import etl.flow.mgr.NodeInfo;
import etl.flow.oozie.coord.COORDINATORAPP;
import etl.flow.oozie.wf.WORKFLOWAPP;

public class OozieFlowMgr extends FlowMgr{
	
	public static final Logger logger = LogManager.getLogger(OozieFlowMgr.class);
	private String dateTimePattern;
	
	private String getDir(FileType ft, String projectName, OozieConf oc){
		if (ft == FileType.actionProperty || ft == FileType.engineProperty || 
				ft==FileType.thirdpartyJar || ft == FileType.ftmappingFile || ft==FileType.log4j){
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
			bdap.xml.config.Configuration.Property oozieLibPath = new bdap.xml.config.Configuration.Property();
			oozieLibPath.setName(OozieConf.key_useSystemPath);
			oozieLibPath.setValue("true");
			bodyConf.getProperty().add(oozieLibPath);
		}{
			bdap.xml.config.Configuration.Property propWfAppPath = new bdap.xml.config.Configuration.Property();
			propWfAppPath.setName(OozieConf.key_oozieWfAppPath);
			propWfAppPath.setValue(String.format("%s/user/%s/%s/%s_workflow.xml", oc.getNameNode(), oc.getUserName(), projectName, flowName));
			bodyConf.getProperty().add(propWfAppPath);
		}
		return bodyConf;
	}

	//deploy all the in-mem files and execute the workflow
	public String deployAndRun(String projectName, String flowName, List<InMemFile> deployFiles, OozieConf oc, EngineConf ec){
		//deploy to the server
		deployFiles.add(super.genEnginePropertyFile(ec));
		FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
		for (InMemFile im:deployFiles){
			String dir = getDir(im.getFileType(), projectName, oc);
			String path = String.format("%s%s", dir, im.getFileName());
			HdfsUtil.writeDfsFile(fs, path, im.getContent());
		}
		//start the job
		String jobSumbitUrl=String.format("http://%s:%d/oozie/v1/jobs", oc.getOozieServerIp(), oc.getOozieServerPort());
		Map<String, String> queryParamMap = new HashMap<String, String>();
		queryParamMap.put(OozieConf.key_oozie_action, OozieConf.value_action_start);
		bdap.xml.config.Configuration bodyConf = getBodyConf(oc, projectName, flowName);
		String body = XmlUtil.marshalToString(bodyConf, "configuration");
		String result = RequestUtil.post(jobSumbitUrl, null, 0, queryParamMap, null, body);
		logger.info(String.format("post result:%s", result));
		Map<String, String> vm = JsonUtil.fromJsonString(result, HashMap.class);
		if (vm!=null){
			return vm.get("id");
		}else{
			return null;
		}
	}
	
	//transform the flow to workflow.xml, actionX.properties, then deploy and run
	@Override
	public String execute(String projectName, Flow flow, List<InMemFile> imFiles, 
			FlowServerConf fsconf, EngineConf ec, String startNode, String instanceId) {
		OozieConf oc = (OozieConf)fsconf;
		//generate wf.xml
		boolean useInstanceId = instanceId==null? false:true;
		if (imFiles==null){
			imFiles = new ArrayList<InMemFile>();
		}
		InMemFile wfXml = genWfXml(flow, startNode, useInstanceId);
		imFiles.add(wfXml);
		//gen action.properties
		List<InMemFile> actionPropertyFiles = super.genProperties(flow);
		imFiles.addAll(actionPropertyFiles);
		String newInstanceId = deployAndRun(projectName, flow.getName(), imFiles, oc, ec);
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
		uploadFiles(projectName, imFiles.toArray(new InMemFile[]{}), fsconf, ec);
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
	public void uploadFiles(String projectName, InMemFile[] files, FlowServerConf fsconf, EngineConf ec) {
		OozieConf oc = (OozieConf) fsconf;
		//deploy to the server
		FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
		for (InMemFile im:files){
			String dir = getDir(im.getFileType(), projectName, oc);
			String path = String.format("%s%s", dir, im.getFileName());
			logger.info(String.format("copy to %s", path));
			HdfsUtil.writeDfsFile(fs, path, im.getContent());
		}
	}

	@Override
	public String getFlowLog(String projectName, FlowServerConf fsconf, String instanceId) {
		OozieConf oc = (OozieConf)fsconf;
		String jobLogUrl=String.format("http://%s:%d/oozie/v2/job/%s?show=log", oc.getOozieServerIp(), oc.getOozieServerPort(), instanceId);
		Map<String, String> headMap = new HashMap<String, String>();
		return RequestUtil.get(jobLogUrl, null, 0, headMap);
	}

	@Override
	public String getNodeLog(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		if (nodeName != null) {
			
		}
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NodeInfo getNodeInfo(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		if (nodeName != null) {
			OozieConf oc = (OozieConf)fsconf;
			String jobLogUrl=String.format("http://%s:%d/oozie/v2/job/%s?show=info", oc.getOozieServerIp(), oc.getOozieServerPort(), instanceId);
			Map<String, String> headMap = new HashMap<String, String>();
			ArrayList<Map<String, String>> actions = JsonUtil.fromJsonString(RequestUtil.get(jobLogUrl, null, 0, headMap), "actions", ArrayList.class);
			if (actions != null) {
				for (Map<String, String> a: actions) {
					if (nodeName.equals(a.get("name"))) {
						return toNodeInfo(a);
					}
				}
			}
		}
		
		return null;
	}

	/* Sample action node info: [cred=null, userRetryMax=0, trackerUri=null, data=null, errorMessage=null, userRetryCount=0, externalChildIDs=null, externalId=null, errorCode=null, conf=<map-reduce xmlns="uri:oozie:workflow:0.5">
	  <job-tracker>${jobTracker}</job-tracker>
	  <name-node>${nameNode}</name-node>
	  <configuration>
	    <property>
	      <name>mapred.mapper.new-api</name>
	      <value>true</value>
	    </property>
	    <property>
	      <name>mapred.reducer.new-api</name>
	      <value>true</value>
	    </property>
	    <property>
	      <name>mapreduce.task.timeout</name>
	      <value>0</value>
	    </property>
	    <property>
	      <name>mapreduce.job.map.class</name>
	      <value>etl.engine.InvokeMapper</value>
	    </property>
	    <property>
	      <name>mapreduce.job.reduces</name>
	      <value>0</value>
	    </property>
	    <property>
	      <name>mapreduce.job.inputformat.class</name>
	      <value>org.apache.hadoop.mapreduce.lib.input.NLineInputFormat</value>
	    </property>
	    <property>
	      <name>mapreduce.input.fileinputformat.inputdir</name>
	      <value>/flow1/sftpcfg/test1.sftp.map.properties</value>
	    </property>
	    <property>
	      <name>mapreduce.job.outputformat.class</name>
	      <value>org.apache.hadoop.mapreduce.lib.output.NullOutputFormat</value>
	    </property>
	    <property>
	      <name>cmdClassName</name>
	      <value>etl.cmd.SftpCmd</value>
	    </property>
	    <property>
	      <name>wfName</name>
	      <value>flow1</value>
	    </property>
	    <property>
	      <name>wfid</name>
	      <value>${wf:id()}</value>
	    </property>
	    <property>
	      <name>staticConfigFile</name>
	      <value>action_sftp.properties</value>
	    </property>
	  </configuration>
	</map-reduce>, type=map-reduce, transition=null, retries=0, consoleUrl=null, stats=null, userRetryInterval=10, name=sftp, startTime=null, toString=Action name[sftp] status[PREP], id=0000008-161103161438380-oozie-play-W@sftp, endTime=null, externalStatus=null, status=PREP]
	*/
	private NodeInfo toNodeInfo(Map<String, String> nodeInfo) {
		NodeInfo n = new NodeInfo();
		DateFormat dtFormat;
		if (dateTimePattern != null)
			dtFormat = new SimpleDateFormat(dateTimePattern);
		else
			dtFormat = DateFormat.getDateTimeInstance();
		n.setNodeId(nodeInfo.get("id"));
		n.setNodeName(nodeInfo.get("name"));
		n.setStatus(nodeInfo.get("status"));
		n.setType(nodeInfo.get("type"));
		String t = nodeInfo.get("startTime");
		if (t != null)
			try {
				n.setStartTime(dtFormat.parse(t));
			} catch (ParseException e) {
				logger.error(e.getMessage(), e);
			}
		t = nodeInfo.get("endTime");
		if (t != null)
			try {
				n.setEndTime(dtFormat.parse(t));
			} catch (ParseException e) {
				logger.error(e.getMessage(), e);
			}
		return n;
	}

	@Override
	public String getNodeOutputFile(String projectName, FlowServerConf fsconf, String instanceId, String nodeName,
			String outputFilePath) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getDateTimePattern() {
		return dateTimePattern;
	}

	public void setDateTimePattern(String dateTimePattern) {
		this.dateTimePattern = dateTimePattern;
	}
}
