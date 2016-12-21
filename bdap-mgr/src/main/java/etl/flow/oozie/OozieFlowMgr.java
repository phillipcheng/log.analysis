package etl.flow.oozie;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import bdap.util.EngineConf;
import bdap.util.FileType;
import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import bdap.util.Util;
import bdap.util.XmlUtil;
import dv.util.RequestUtil;
import etl.flow.CoordConf;
import etl.flow.Data;
import etl.flow.Flow;
import etl.flow.InputFormatType;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.mgr.InMemFile;
import etl.flow.mgr.NodeInfo;
import etl.flow.oozie.wf.WORKFLOWAPP;

public class OozieFlowMgr extends FlowMgr{
	
	public static final Logger logger = LogManager.getLogger(OozieFlowMgr.class);
	private static final int DEFAULT_MAX_FILE_SIZE = 1048576; /* 1MB */
	private static final String[] EMPTY_STRING_ARRAY = new String[0];
	private static final InMemFile[] EMPTY_LOG_FILES = new InMemFile[0];
	private String dateTimePattern;
	
	
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
	
	private bdap.xml.config.Configuration.Property getWfIdConf(String wfId){
		bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
		property.setName(OozieConf.key_wfInstanceId);
		property.setValue(wfId);
		return property;
	}
	
	/*
	oozie.libpath=${nameNode}/user/${user.name}/bdap-VVERSIONN/lib/
	oozie.wf.application.path=${nameNode}/user/${user.name}/pde-VVERSIONN/binfile/binfile_workflow.xml
	note: ${nameNode}/user/${user.name}/pde-VVERSIONN/binfile/lib/ not needed in oozie.libpath, as can be implied from oozie.wf.application.path
	 */
	private bdap.xml.config.Configuration getWfConf(OozieConf oc, String projectDir, String flowName){
		bdap.xml.config.Configuration bodyConf = new bdap.xml.config.Configuration();
		{
			bdap.xml.config.Configuration.Property oozieLibPath = new bdap.xml.config.Configuration.Property();
			oozieLibPath.setName(OozieConf.key_oozieLibPath);
			oozieLibPath.setValue(oc.getOozieLibPath());
			bodyConf.getProperty().add(oozieLibPath);
		}{
			bdap.xml.config.Configuration.Property propWfAppPath = new bdap.xml.config.Configuration.Property();
			propWfAppPath.setName(OozieConf.key_oozieWfAppPath);
			String dir = getDir(FileType.oozieWfXml, projectDir, flowName, oc);
			propWfAppPath.setValue(String.format("%s%s_workflow.xml", dir, flowName));
			bodyConf.getProperty().add(propWfAppPath);
		}
		return bodyConf;
	}
	
	/*
	oozie.libpath=${nameNode}/user/${user.name}/bdap-VVERSIONN/lib/,${nameNode}/user/${user.name}/pde-VVERSIONN/binfile/lib/
	oozie.coord.application.path=${nameNode}/user/${user.name}/pde-VVERSIONN/binfile/binfile_coordinator.xml
	workflowAppUri=${nameNode}/user/${user.name}/pde-VVERSIONN/binfile/binfile_workflow.xml
	flowName=test1
	duration=15
	start=2016-09-21T08:40Z
	end=2020-10-27T09:00Z
	*/
	private bdap.xml.config.Configuration getCoordConf(OozieConf oc, CoordConf cc, String projectDir, String flowName){
		bdap.xml.config.Configuration bodyConf = new bdap.xml.config.Configuration();
		{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			String projectLibPath = getDir(FileType.thirdpartyJar, projectDir, flowName, oc);
			property.setName(OozieConf.key_oozieLibPath);
			property.setValue(String.format("%s,%s", oc.getOozieLibPath(), projectLibPath));
			bodyConf.getProperty().add(property);
		}{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName(OozieConf.key_oozieCoordinateAppPath);
			property.setValue(cc.getCoordPath());
			bodyConf.getProperty().add(property);
		}{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName("workflowAppUri");
			String dir = getDir(FileType.oozieWfXml, projectDir, flowName, oc);
			property.setValue(String.format("%s%s_workflow.xml", dir, flowName));
			bodyConf.getProperty().add(property);
		}{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName("flowName");
			property.setValue(flowName);
			bodyConf.getProperty().add(property);
		}{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName("duration");
			property.setValue(String.valueOf(cc.getDuration()));
			bodyConf.getProperty().add(property);
		}{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName("start");
			property.setValue(String.valueOf(cc.getStartTime()));
			bodyConf.getProperty().add(property);
		}{
			bdap.xml.config.Configuration.Property property = new bdap.xml.config.Configuration.Property();
			property.setName("end");
			property.setValue(String.valueOf(cc.getEndTime()));
			bodyConf.getProperty().add(property);
		}
		return bodyConf;
	}

	//deploy all the in-mem files
	public boolean deployFlowFromXml(String projectDir, String flowName, List<InMemFile> deployFiles, OozieConf oc, EngineConf ec){
		//deploy to the server
		deployFiles.add(super.genEnginePropertyFile(ec));
		FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
		for (InMemFile im:deployFiles){
			String dir = getDir(im.getFileType(), projectDir, flowName, oc);
			String path = String.format("%s%s", dir, im.getFileName());
			HdfsUtil.writeDfsFile(fs, path, im.getContent());
		}
		return true;
	}
	
	//generate the wf.xml, action.properties, etlengine.propertis, and deploy it
	@Override
	public boolean deployFlowFromJson(String prjName, Flow flow, FlowDeployer fd) {
		List<InMemFile> imFiles = new ArrayList<InMemFile>();
		//gen wf xml
		InMemFile wfXml = genWfXml(flow, null, false);
		imFiles.add(wfXml);
		//gen action.properties
		List<InMemFile> actionPropertyFiles = super.genProperties(flow);
		imFiles.addAll(actionPropertyFiles);
		//gen etlengine.properties
		InMemFile enginePropertyFile = super.genEnginePropertyFile(fd.getEngineConfig());
		imFiles.add(enginePropertyFile);
		//deploy to the server
		String projectDir = fd.getProjectHdfsDir(prjName);
		uploadFiles(projectDir, flow.getName(), imFiles.toArray(new InMemFile[]{}), fd.getOozieServerConf(), fd);
		return true;
	}

	@Override
	public String executeFlow(String prjName, String flowName, FlowDeployer fd){
		String projectDir = fd.getProjectHdfsDir(prjName);
		OozieConf oc = fd.getOozieServerConf();
		String jobSumbitUrl=String.format("http://%s:%d/oozie/v1/jobs", oc.getOozieServerIp(), oc.getOozieServerPort());
		Map<String, String> queryParamMap = new HashMap<String, String>();
		queryParamMap.put(OozieConf.key_oozie_action, OozieConf.value_action_start);
		bdap.xml.config.Configuration commonConf = getCommonConf(oc, prjName, flowName);
		bdap.xml.config.Configuration wfConf = getWfConf(oc, projectDir, flowName);
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
	
	public String executeFlow(String prjName, String flowName, FlowDeployer fd, String wfId){
		String projectDir = fd.getProjectHdfsDir(prjName);
		OozieConf oc = fd.getOozieServerConf();
		String jobSumbitUrl=String.format("http://%s:%d/oozie/v1/jobs", oc.getOozieServerIp(), oc.getOozieServerPort());
		Map<String, String> queryParamMap = new HashMap<String, String>();
		queryParamMap.put(OozieConf.key_oozie_action, OozieConf.value_action_start);
		bdap.xml.config.Configuration commonConf = getCommonConf(oc, prjName, flowName);
		bdap.xml.config.Configuration wfConf = getWfConf(oc, projectDir, flowName);
		commonConf.getProperty().addAll(wfConf.getProperty());
		bdap.xml.config.Configuration.Property wfIdProperty = getWfIdConf(wfId);
		commonConf.getProperty().add(wfIdProperty);
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
	public String executeCoordinator(String prjName, String flowName, FlowDeployer fd, CoordConf cc){
		String projectDir = fd.getProjectHdfsDir(prjName);
		OozieConf oc = fd.getOozieServerConf();
		//start the coordinator
		String jobSumbitUrl=String.format("http://%s:%d/oozie/v1/jobs", oc.getOozieServerIp(), oc.getOozieServerPort());
		Map<String, String> headMap = new HashMap<String, String>();
		bdap.xml.config.Configuration commonConf = getCommonConf(oc, prjName, flowName);
		bdap.xml.config.Configuration coordConf = getCoordConf(oc, cc, projectDir, flowName);
		commonConf.getProperty().addAll(coordConf.getProperty());
		String body = XmlUtil.marshalToString(commonConf, "configuration");
		String ret = RequestUtil.post(jobSumbitUrl, headMap, body);
		Map<String, String> vm = JsonUtil.fromJsonString(ret, HashMap.class);
		if (vm!=null){
			return vm.get("id");
		}else{
			return null;
		}
	}

	@Override
	public void executeAction(String projectName, Flow flow, List<InMemFile> imFiles, FlowServerConf fsconf,
			EngineConf ec, String actionNode, String instanceId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public FlowInfo getFlowInfo(String projectName, FlowServerConf fsconf, String instanceId) {
		if (instanceId != null) {
			OozieConf oc = (OozieConf)fsconf;
			String jobInfoUrl=String.format("http://%s:%d/oozie/v2/job/%s?show=info", oc.getOozieServerIp(), oc.getOozieServerPort(), instanceId);
			Map<String, String> headMap = new HashMap<String, String>();
			Map<String, Object> flowInfoMap = JsonUtil.fromJsonString(RequestUtil.get(jobInfoUrl, null, 0, headMap), HashMap.class);
			FlowInfo flowInfo = new FlowInfo();
			flowInfo.setId(instanceId);
			flowInfo.setName((String) flowInfoMap.get("appName"));
			flowInfo.setStatus((String) flowInfoMap.get("status"));
			DateFormat dtFormat;
			if (dateTimePattern != null)
				dtFormat = new SimpleDateFormat(dateTimePattern, Locale.forLanguageTag("en-US"));
			else
				dtFormat = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.forLanguageTag("en-US"));
			String t = (String) flowInfoMap.get("startTime");
			flowInfo.setStartTime(t);
			t = (String) flowInfoMap.get("endTime");
			flowInfo.setEndTime(t);
			t = (String) flowInfoMap.get("createdTime");
			flowInfo.setCreatedTime(t);
			t = (String) flowInfoMap.get("lastModTime");
			flowInfo.setLastModifiedTime(t);
			return flowInfo;
		}
		return null;
	}

	@Override
	public String getFlowLog(String projectName, FlowServerConf fsconf, String instanceId) {
		OozieConf oc = (OozieConf)fsconf;
		String jobLogUrl=String.format("http://%s:%d/oozie/v2/job/%s?show=log", oc.getOozieServerIp(), oc.getOozieServerPort(), instanceId);
		Map<String, String> headMap = new HashMap<String, String>();
		return RequestUtil.get(jobLogUrl, null, 0, headMap);
	}

	@Override
	public InMemFile[] getNodeLog(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		if (nodeName != null) {
			OozieConf oc = (OozieConf)fsconf;
			String jobInfoUrl=String.format("http://%s:%d/oozie/v2/job/%s?show=info", oc.getOozieServerIp(), oc.getOozieServerPort(), instanceId);
			Map<String, String> headMap = new HashMap<String, String>();
			ArrayList<Map<String, Object>> actions = JsonUtil.fromJsonString(RequestUtil.get(jobInfoUrl, null, 0, headMap), "actions", ArrayList.class);
			if (actions != null) {
				Map<String, Object> action = null;
				for (Map<String, Object> a: actions) {
					if (nodeName.equals(a.get("name"))) {
						action = a;
						break;
					}
				}
				
				if (action != null) {
					String launcherJobId = null;
					String childJobIds[] = null;
					List<InMemFile> logFiles = new ArrayList<InMemFile>();
					ArrayList<Map<String, Object>> jobAttempts;
					String url;
					Object t;
					t = action.get("externalId");
					if (t != null)
						launcherJobId = t.toString();
					t = action.get("externalChildIDs");
					if (t != null)
						childJobIds = t.toString().split(",");
					
					if (launcherJobId != null) {
						url = String.format("%s/ws/v1/history/mapreduce/jobs/%s/jobattempts", oc.getHistoryServer(), launcherJobId);
						jobAttempts = JsonUtil.fromJsonString(RequestUtil.get(url, null, 0, headMap), "jobAttempts.jobAttempt", ArrayList.class);
						
						for (Map<String, Object> a: jobAttempts) {
							appendLog(logFiles, FileType.stderrLog, a.get("logsLink") + "/stderr?start=0");
							appendLog(logFiles, FileType.stdoutLog, a.get("logsLink") + "/stdout?start=0");
							appendLog(logFiles, FileType.sysLog, a.get("logsLink") + "/syslog?start=0");
						}
					}
					
					if (childJobIds != null) {
						ArrayList<Map<String, Object>> tasks;
						ArrayList<Map<String, Object>> taskAttempts;
						String user;
						for (String id: childJobIds) {
							url = String.format("%s/ws/v1/history/mapreduce/jobs/%s", oc.getHistoryServer(), id);
							user = JsonUtil.fromJsonString(RequestUtil.get(url, null, 0, headMap), "job.user", String.class);
							
							/* Application master attempts */
							url = String.format("%s/ws/v1/history/mapreduce/jobs/%s/jobattempts", oc.getHistoryServer(), id);
							jobAttempts = JsonUtil.fromJsonString(RequestUtil.get(url, null, 0, headMap), "jobAttempts.jobAttempt", ArrayList.class);
							
							for (Map<String, Object> a: jobAttempts) {
								appendLog(logFiles, FileType.stderrLog, a.get("logsLink") + "/stderr?start=0");
								appendLog(logFiles, FileType.stdoutLog, a.get("logsLink") + "/stdout?start=0");
								appendLog(logFiles, FileType.sysLog, a.get("logsLink") + "/syslog?start=0");
							}
							
							/* Task attempts */
							url = String.format("%s/ws/v1/history/mapreduce/jobs/%s/tasks", oc.getHistoryServer(), id);
							tasks = JsonUtil.fromJsonString(RequestUtil.get(url, null, 0, headMap), "tasks.task", ArrayList.class);
							if (tasks != null) {
								for (Map<String, Object> task: tasks) {
									url = String.format("%s/ws/v1/history/mapreduce/jobs/%s/tasks/%s/attempts", oc.getHistoryServer(), id, task.get("id"));
									taskAttempts = JsonUtil.fromJsonString(RequestUtil.get(url, null, 0, headMap), "taskAttempts.taskAttempt", ArrayList.class);
									if (taskAttempts != null) {
										for (Map<String, Object> taskAttempt: taskAttempts) {
											url = String.format("%s/jobhistory/logs/%s/%s/%s/%s",
													oc.getHistoryServer(), nodeAddressToId(fsconf, (String)taskAttempt.get("nodeHttpAddress")),
													(String)taskAttempt.get("assignedContainerId"), (String)taskAttempt.get("id"), user);

											appendLog(logFiles, FileType.stderrLog, url + "/stderr?start=0");
											appendLog(logFiles, FileType.stdoutLog, url + "/stdout?start=0");
											appendLog(logFiles, FileType.sysLog, url + "/syslog?start=0");
										}
									}
								}
							}
						}
					}
					
					return logFiles.toArray(EMPTY_LOG_FILES);
				}
			}
		}
		return EMPTY_LOG_FILES;
	}
	
	private String nodeAddressToId(FlowServerConf fsconf, String nodeAddress) {
		/* TODO: Node id mapping history */
		Map<String, String> headMap = new HashMap<String, String>();
		OozieConf oc = (OozieConf)fsconf;
		String url = String.format("%s/ws/v1/cluster/nodes", oc.getRmWebApp());
		ArrayList<Map<String, Object>> nodes = JsonUtil.fromJsonString(RequestUtil.get(url, null, 0, headMap), "nodes.node", ArrayList.class);
		if (nodes != null) {
			for (Map<String, Object> n: nodes) {
				if (nodeAddress.equals(n.get("nodeHTTPAddress")))
					return (String)n.get("id");
			}
		}
		return "";
	}

	private void appendLog(List<InMemFile> logFiles, FileType fileType, String url) {
		Map<String, String> headMap = new HashMap<String, String>();
		String log = RequestUtil.get(url, null, 0, headMap);
		int logStart = log.indexOf("<pre>");
		int logEnd = log.indexOf("</pre>");
		if (logStart != -1 && logEnd != -1)
			log = log.substring(logStart + 5, logEnd);
		else
			log = "";
		
		InMemFile logFile = new InMemFile();
		logFile.setFileName(url);
		if (fileType != null)
			/* Log file type STDOUT/ERROR/SYS */
			logFile.setFileType(fileType);
		else
			logFile.setFileType(FileType.textData);
		logFile.setTextContent(log);
		logFiles.add(logFile);
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
			dtFormat = new SimpleDateFormat(dateTimePattern, Locale.forLanguageTag("en-US"));
		else
			dtFormat = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.forLanguageTag("en-US"));
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
	public String[] listNodeInputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId, String nodeName) {
		if (nodeName != null) {
			OozieConf oc = (OozieConf)fsconf;
			String jobLogUrl=String.format("http://%s:%d/oozie/v2/job/%s?show=info", oc.getOozieServerIp(), oc.getOozieServerPort(), instanceId);
			Map<String, String> headMap = new HashMap<String, String>();
			ArrayList<Map<String, Object>> actions = JsonUtil.fromJsonString(RequestUtil.get(jobLogUrl, null, 0, headMap), "actions", ArrayList.class);
			if (actions != null) {
				Map<String, Object> action = null;
				for (Map<String, Object> a: actions) {
					if (nodeName.equals(a.get("name"))) {
						action = a;
						break;
					}
				}
				
				if (action != null) {
					String childJobIds[] = null;
					String url;
					Object t = action.get("externalChildIDs");
					if (t != null)
						childJobIds = t.toString().split(",");
					
					if (childJobIds != null) {
						List<Map<String,Object>> properties;
						List<String> inputFiles = new ArrayList<String>();
						for (String id: childJobIds) {
							url = String.format("%s/ws/v1/history/mapreduce/jobs/%s/conf", oc.getHistoryServer(), id);
							properties = JsonUtil.fromJsonString(RequestUtil.get(url, null, 0, headMap), "conf.property", ArrayList.class);
							for (Map<String, Object> prop: properties) {
								if ("mapreduce.input.fileinputformat.inputdir".equals(prop.get("name"))) {
									inputFiles.addAll(listDFSDir(ec, (String)prop.get("value")));
									break;
								}
							}
						}
						return inputFiles.toArray(EMPTY_STRING_ARRAY);
					}
				}
			}
		}
		return EMPTY_STRING_ARRAY;
	}

	@Override
	public String[] listNodeOutputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId, String nodeName) {
		if (nodeName != null) {
			OozieConf oc = (OozieConf)fsconf;
			String jobLogUrl=String.format("http://%s:%d/oozie/v2/job/%s?show=info", oc.getOozieServerIp(), oc.getOozieServerPort(), instanceId);
			Map<String, String> headMap = new HashMap<String, String>();
			ArrayList<Map<String, Object>> actions = JsonUtil.fromJsonString(RequestUtil.get(jobLogUrl, null, 0, headMap), "actions", ArrayList.class);
			if (actions != null) {
				Map<String, Object> action = null;
				for (Map<String, Object> a: actions) {
					if (nodeName.equals(a.get("name"))) {
						action = a;
						break;
					}
				}
				
				if (action != null) {
					String childJobIds[] = null;
					String url;
					Object t = action.get("externalChildIDs");
					if (t != null)
						childJobIds = t.toString().split(",");
					
					if (childJobIds != null) {
						List<Map<String,Object>> properties;
						List<String> inputFiles = new ArrayList<String>();
						for (String id: childJobIds) {
							url = String.format("%s/ws/v1/history/mapreduce/jobs/%s/conf", oc.getHistoryServer(), id);
							properties = JsonUtil.fromJsonString(RequestUtil.get(url, null, 0, headMap), "conf.property", ArrayList.class);
							for (Map<String, Object> prop: properties) {
								if ("mapreduce.output.fileoutputformat.outputdir".equals(prop.get("name"))) {
									inputFiles.addAll(listDFSDir(ec, (String)prop.get("value")));
									break;
								}
							}
						}
						return inputFiles.toArray(EMPTY_STRING_ARRAY);
					}
				}
			}
		}
		return EMPTY_STRING_ARRAY;
	}

	private List<String> listDFSDir(EngineConf ec, String remoteDir) {
		if (remoteDir != null) {
			FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
			try {
				if (fs.isDirectory(new Path(remoteDir))) {
					final Function<String, String> FULL_PATH_TRANSFORMER = new Function<String, String>() {
					    public String apply(String fileName) {
					    	if (remoteDir.endsWith(Path.SEPARATOR))
					    		return remoteDir + fileName;
					    	else
					    		return remoteDir + Path.SEPARATOR + fileName;
					    }
					};
					return Lists.transform(HdfsUtil.listDfsFile(fs, remoteDir), FULL_PATH_TRANSFORMER);
				} else {
					return Lists.newArrayList(remoteDir);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			return Collections.emptyList();
			
		} else
			return Collections.emptyList();
	}
	
	private InMemFile getDFSFile(FileSystem fs, String filePath, InputFormatType dataFormat) {
		Path p = new Path(filePath);
		try {
			if (fs.exists(p)) {
				if (fs.isFile(p)) {
					if (textType(dataFormat))
						return new InMemFile(FileType.textData, filePath, HdfsUtil.readDfsTextFile(fs, filePath, DEFAULT_MAX_FILE_SIZE));
					else
						return new InMemFile(FileType.binaryData, filePath, HdfsUtil.readDfsFile(fs, filePath, DEFAULT_MAX_FILE_SIZE));
				} else {
					return new InMemFile(FileType.directoryList, filePath, directoryList(HdfsUtil.listDfsFilePath(fs, filePath, true)));
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		
		return null;
	}

	@Override
	public InMemFile getDFSFile(EngineConf ec, Data data) {
		if (data != null && !data.isInstance() && data.getLocation() != null && data.getLocation().length() > 0) {
			FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
			
			return getDFSFile(fs, data.getLocation(), data.getDataFormat());
		}
		return null;
	}

	@Override
	public InMemFile getDFSFile(EngineConf ec, Data data, FlowInfo flowInfo) {
		if (data != null && data.isInstance() && data.getLocation() != null && data.getLocation().length() > 0) {
			FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
			String filePath = data.getLocation();
			if (!filePath.endsWith(Path.SEPARATOR))
				filePath += Path.SEPARATOR;
			
			if (flowInfo != null && flowInfo.getId() != null)
				filePath += flowInfo.getId();
			
			return getDFSFile(fs, filePath, data.getDataFormat());
		}
		return null;
	}
	
	private String directoryList(List<String> list) {
		StringBuilder buffer = new StringBuilder();
		if (list != null) {
			for (String f: list) {
				buffer.append(f);
				buffer.append("\n");
			}
		}
		return buffer.toString();
	}

	private boolean textType(InputFormatType dataFormat) {
		if (dataFormat == null)
			return false;
		else if (InputFormatType.Binary.equals(dataFormat))
			return false;
		else
			return true;
	}

	@Override
	public InMemFile getDFSFile(EngineConf ec, String filePath) {
		return getDFSFile(ec, filePath, DEFAULT_MAX_FILE_SIZE);
	}
	
	@Override
	public InMemFile getDFSFile(EngineConf ec, String filePath, int maxFileSize) {
		if (filePath != null) {
			FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
			if (FileType.textData.equals(Util.guessFileType(filePath))) {
				String text = HdfsUtil.readDfsTextFile(fs, filePath, DEFAULT_MAX_FILE_SIZE);
				if (text != null)
					return new InMemFile(FileType.textData, filePath, text);
				else
					return null;
			} else {
				byte[] binary = HdfsUtil.readDfsFile(fs, filePath, DEFAULT_MAX_FILE_SIZE);
				if (binary != null)
					return new InMemFile(FileType.binaryData, filePath, binary);
				else
					return null;
			}
		}
		return null;
	}
	
	public boolean putDFSFile(EngineConf ec, String filePath, InMemFile file) {
		if (filePath != null) {
			FileSystem fs = HdfsUtil.getHadoopFs(ec.getDefaultFs());
			if (FileType.textData.equals(file.getFileType()))
				return HdfsUtil.writeDfsFile(fs, filePath, file.getTextContent().getBytes(StandardCharsets.UTF_8), false);
			else
				return HdfsUtil.writeDfsFile(fs, filePath, file.getContent(), false);
		} else
			return false;
	}

	public String getDateTimePattern() {
		return dateTimePattern;
	}

	public void setDateTimePattern(String dateTimePattern) {
		this.dateTimePattern = dateTimePattern;
	}
}
