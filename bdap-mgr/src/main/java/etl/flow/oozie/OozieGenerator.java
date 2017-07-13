package etl.flow.oozie;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.EngineConf;
import etl.engine.ETLCmd;
import etl.engine.types.DataType;
import etl.engine.types.InputFormatType;
import etl.flow.ActionNode;
import etl.flow.CallSubFlowNode;
import etl.flow.CoordConf;
import etl.flow.Data;
import etl.flow.EndNode;
import etl.flow.ExeType;
import etl.flow.Flow;
import etl.flow.Link;
import etl.flow.Node;
import etl.flow.NodeLet;
import etl.flow.StartNode;
import etl.flow.deploy.EngineType;
import etl.flow.deploy.FlowDeployer;
import etl.flow.oozie.coord.COORDINATORAPP;

import etl.flow.oozie.wf.ACTION;
import etl.flow.oozie.wf.ACTIONTRANSITION;
import etl.flow.oozie.wf.CONFIGURATION;
import etl.flow.oozie.wf.DELETE;
import etl.flow.oozie.wf.END;
import etl.flow.oozie.wf.FLAG;
import etl.flow.oozie.wf.FORK;
import etl.flow.oozie.wf.FORKTRANSITION;
import etl.flow.oozie.wf.JAVA;
import etl.flow.oozie.wf.JOIN;
import etl.flow.oozie.wf.KILL;
import etl.flow.oozie.wf.MAPREDUCE;
import etl.flow.oozie.wf.PARAMETERS;
import etl.flow.oozie.wf.PREPARE;
import etl.flow.oozie.wf.START;
import etl.flow.oozie.wf.SUBWORKFLOW;
import etl.flow.oozie.wf.WORKFLOWAPP;
import etl.util.GlobExpPathFilter;

public class OozieGenerator {
	public static final Logger logger = LogManager.getLogger(OozieGenerator.class);
	
	public static final String jobTrackValue="${jobTracker}";
	public static final String nameNodeValue="${nameNode}";
	
	//common
	public static final String prop_mapper_new ="mapred.mapper.new-api";
	public static final String prop_reducer_new ="mapred.reducer.new-api";
	public static final String prop_task_timeout ="mapreduce.task.timeout";
	//mapper reducer
	public static final String prop_map_class ="mapreduce.job.map.class";
	public static final String prop_reduce_class ="mapreduce.job.reduce.class";
	public static final String prop_reduce_num ="mapreduce.job.reduces";//set this to 0 when there is no reducer
	//input output
	public static final String prop_inputformat="mapreduce.job.inputformat.class";
		
	public static final String prop_inputdirs="mapreduce.input.fileinputformat.inputdir";
	public static final String prop_outputformat="mapreduce.job.outputformat.class";
		public static final String prop_outputformat_null="org.apache.hadoop.mapreduce.lib.output.NullOutputFormat";
	public static final String prop_outputdirs="mapreduce.output.fileoutputformat.outputdir";
	public static final String prop_output_keyclass="mapreduce.job.output.key.class";
	public static final String prop_output_valueclass="mapreduce.job.output.value.class";
		public static final String prop_text_type="org.apache.hadoop.io.Text";
		public static final String prop_void_type="java.lang.Void";
	public static final String prop_input_pathfilter="mapreduce.input.pathFilter.class";
		public static final String prop_input_path_globexpfilter="etl.util.GlobExpPathFilter";
	//command parameter are defined in the InvokerMapper
	
	private static String killName = "fail";
	private static String killMessage = "failed, error message[${wf:errorMessage(wf:lastErrorNode())}]";
	private static ACTIONTRANSITION errorTransition = new ACTIONTRANSITION();
	public static final String wfid = "${wf:id()}";
	public static final String wfid_in_fun = "wf:id()";
	public static final String wfid_param_name="wfid";
	public static final String wfid_param_exp= "${wfid}";
	
	private static ETLCmd getCmd(ActionNode an) {
		try{
			String cmdClazz = (String) an.getProperties().get(ActionNode.key_cmd_class);
			return (ETLCmd) Thread.currentThread().getContextClassLoader().loadClass(cmdClazz).newInstance();
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	private static String getInputDir(Data d, InputFormatType ift, DataType dt,List<String> pathFilter){
		String baseoutput = null;
		if (d.getBaseOutput()==null){
			baseoutput="*";
		}else{
			baseoutput=d.getBaseOutput()+"-*";
		}
		String inputDir=null;
		if (d.isInstance()){
			if (Data.INTANCE_FLOW_ME.equals(d.getInstanceFlow())){
				if (ift!=InputFormatType.FileName && dt==DataType.Path){
					//the content of the input are path, we need to get the content of those paths
					//${getContentsFromDfsFiles(nameNode, concat(concat('/femtocell/femto/filenameupdate_output/',wf:id()),'/'))}
					if(pathFilter!=null && pathFilter.size()>0){
						inputDir = String.format("${getContentsFromDfsFilesByPathFilter(nameNode, concat(concat('%s',%s),'/%s'),'%s')}", 
								d.getLocation(), wfid_in_fun, baseoutput,String.join(",", pathFilter));
					}else{
						inputDir = String.format("${getContentsFromDfsFiles(nameNode, concat(concat('%s',%s),'/%s'))}", 
								d.getLocation(), wfid_in_fun, baseoutput);
					}
				}else{
					inputDir = String.format("%s%s/%s",d.getLocation(),wfid,baseoutput);
				}
			}else{
				if (ift!=InputFormatType.FileName && dt==DataType.Path){
					//TODO
					throw new UnsupportedOperationException();
				}else{
					///flow1/csvmerge/${wf:actionExternalId('call_flow1')}
					inputDir = d.getLocation()+String.format("${wf:actionExternalId('%s')}", d.getInstanceFlow())+"/*";
				}
			}
		}else{
			inputDir = d.getLocation();
		}
		return inputDir;
	}
	
	private static MAPREDUCE genMRAction(Flow flow, ActionNode an, boolean hasInstanceId){
		MAPREDUCE mr = new MAPREDUCE();
		mr.setJobTracker(jobTrackValue);
		mr.setNameNode(nameNodeValue);
		CONFIGURATION cfg = new CONFIGURATION();
		mr.setConfiguration(cfg);
		List<CONFIGURATION.Property> pl = cfg.getProperty();
		//common configuration
		CONFIGURATION.Property mapperNewCp = new CONFIGURATION.Property();
		mapperNewCp.setName(prop_mapper_new);
		mapperNewCp.setValue("true");
		pl.add(mapperNewCp);
		CONFIGURATION.Property reducerNewCp = new CONFIGURATION.Property();
		reducerNewCp.setName(prop_reducer_new);
		reducerNewCp.setValue("true");
		pl.add(reducerNewCp);
		CONFIGURATION.Property taskTimeoutCp = new CONFIGURATION.Property();
		taskTimeoutCp.setName(prop_task_timeout);
		taskTimeoutCp.setValue("0");
		pl.add(taskTimeoutCp);
		//mapper/reducer configuration
		CONFIGURATION.Property mapperClassCp = new CONFIGURATION.Property();
		mapperClassCp.setName(prop_map_class);
		mapperClassCp.setValue(EngineConf.mapper_class);
		pl.add(mapperClassCp);
		ETLCmd cmd = getCmd(an);
		if (cmd==null){
			logger.error(String.format("%s is not supported.\n%s", an.getName(), an.getProperties()));
			return null;
		}
		if (cmd.hasReduce()){
			CONFIGURATION.Property reducerClassCp = new CONFIGURATION.Property();
			reducerClassCp.setName(prop_reduce_class);
			reducerClassCp.setValue(EngineConf.reducer_class);
			pl.add(reducerClassCp);
		}else{
			CONFIGURATION.Property reducerNumCp = new CONFIGURATION.Property();
			reducerNumCp.setName(prop_reduce_num);
			reducerNumCp.setValue("0");
			pl.add(reducerNumCp);
		}
		//input and output configuration
		List<NodeLet> inlets = an.getInLets();
		List<String> inputDataDirs = new ArrayList<String>();
		//add system properties
		Map<String, Object> sysProperties = an.getSysProperties();
		List<String> pathFilter = new ArrayList<String>();
		for (String key: sysProperties.keySet()){
			if (key.equals(GlobExpPathFilter.cfgkey_path_filters)){
				CONFIGURATION.Property add = new CONFIGURATION.Property();
				add.setName(prop_input_pathfilter);
				add.setValue(prop_input_path_globexpfilter);
				pl.add(add);
				pathFilter.add(String.valueOf(sysProperties.get(key)));
			}
			CONFIGURATION.Property cp = new CONFIGURATION.Property();
			cp.setName(key);
			cp.setValue(String.valueOf(sysProperties.get(key)));
			pl.add(cp);
			
		}
		//all the input dataset to this action should have the same inputformattype, datatype/recordtype
		InputFormatType ift = null;
		DataType dt = null;
		String aift = (String) an.getProperty(ETLCmd.cfgkey_input_format, EngineType.oozie);
		String adt = (String) an.getProperty(ETLCmd.cfgkey_record_type, EngineType.oozie);
		for (NodeLet ln: inlets){
			if (ln.getDataName()!=null){
				Data d = flow.getDataDef(ln.getDataName());
				if (d==null){
					logger.error(String.format("data %s not found.", ln.getDataName()));
					return null;
				}else{
					if (ift==null){
						ift = d.getDataFormat();
						dt = d.getRecordType();
						if (aift!=null){
							ift = InputFormatType.valueOf(aift);
						}
						if (adt!=null){
							dt = DataType.valueOf(adt);
						}
					}else{
						if (aift==null && !ift.equals(d.getDataFormat())){
							logger.error(String.format("all input data should have the same inputformattype, %s differ with %s in action %s", 
									d, ift, an.getName()));
							return null;
						}
						if (adt==null && !dt.equals(d.getRecordType())){
							logger.error(String.format("all input data should have the same datatype, %s differ with %s in action %s", 
									d, dt, an.getName()));
							return null;
						}
					}
					inputDataDirs.add(getInputDir(d, ift, dt,pathFilter));
				}
			}
		}
		//input properties
		CONFIGURATION.Property inputFormatTypeCp = new CONFIGURATION.Property();
		inputFormatTypeCp.setName(prop_inputformat);
		inputFormatTypeCp.setValue(ETLCmd.getInputFormat(ift).getName());
		pl.add(inputFormatTypeCp);
		if (dt == DataType.KeyPath || dt == DataType.KeyValue){
			CONFIGURATION.Property useKeyValueCp = new CONFIGURATION.Property();
			useKeyValueCp.setName(ETLCmd.sys_cfgkey_use_keyvalue);
			useKeyValueCp.setValue("true");
			pl.add(useKeyValueCp);
		}
		//
		CONFIGURATION.Property inputDirsCp = new CONFIGURATION.Property();
		inputDirsCp.setName(prop_inputdirs);
		inputDirsCp.setValue(String.join(",", inputDataDirs));
		pl.add(inputDirsCp);
		//output properties
		CONFIGURATION.Property outputFormatCp = new CONFIGURATION.Property();
		outputFormatCp.setName(prop_outputformat);
		CONFIGURATION.Property outputDirCp = new CONFIGURATION.Property();
		outputDirCp.setName(prop_outputdirs);
		List<NodeLet> outlets = an.getOutlets();
		String outputDataDir=null;
		if (outlets!=null && outlets.size()>0){//for multiple output, the location should be the same, only differ baseoutput
			String dataName = outlets.iterator().next().getDataName();
			if (dataName!=null){
				Data d = flow.getDataDef(dataName);
				if (d==null){
					logger.error(String.format("data not found:%s", dataName));
					return null;
				}
				if (Data.INTANCE_FLOW_ME.equals(d.getInstanceFlow())){
					outputDataDir = d.getLocation()+wfid;
				}else{
					outputDataDir = d.getLocation()+String.format("${wf:actionExternalId('%s')}", d.getInstanceFlow());
				}
				outputFormatCp.setValue(FlowDeployer.getOutputFormat(an));
				outputDirCp.setValue(outputDataDir);
				pl.add(outputDirCp);
				{
					CONFIGURATION.Property outputKeyCp = new CONFIGURATION.Property();
					outputKeyCp.setName(prop_output_keyclass);
					outputKeyCp.setValue(prop_text_type);
					pl.add(outputKeyCp);
				}{
					CONFIGURATION.Property outputValueCp = new CONFIGURATION.Property();
					outputValueCp.setName(prop_output_valueclass);
					outputValueCp.setValue(prop_text_type);
					pl.add(outputValueCp);
				}
				//add prepare
				DELETE del = new DELETE();
				del.setPath(outputDataDir);
				PREPARE prepare = new PREPARE();
				mr.setPrepare(prepare);
				mr.getPrepare().getDelete().add(del);
			}else{
				outputFormatCp.setValue(prop_outputformat_null);
			}
		}else{//
			outputFormatCp.setValue(prop_outputformat_null);
		}
		pl.add(outputFormatCp);
		//cmd configuration
		CONFIGURATION.Property cmdClassNameCp = new CONFIGURATION.Property();
		cmdClassNameCp.setName(EngineConf.cfgkey_cmdclassname);
		cmdClassNameCp.setValue(cmd.getClass().getName());
		pl.add(cmdClassNameCp);
		CONFIGURATION.Property wfNameCp = new CONFIGURATION.Property();
		wfNameCp.setName(EngineConf.cfgkey_wfName);
		wfNameCp.setValue(flow.getName());
		pl.add(wfNameCp);
		CONFIGURATION.Property wfIdCp = new CONFIGURATION.Property();
		wfIdCp.setName(EngineConf.cfgkey_wfid);
		if (!hasInstanceId){
			wfIdCp.setValue(wfid);
		}else{
			wfIdCp.setValue(wfid_param_exp);
		}
		pl.add(wfIdCp);
		CONFIGURATION.Property cfgPropertiesCp = new CONFIGURATION.Property();
		cfgPropertiesCp.setName(EngineConf.cfgkey_staticconfigfile);
		cfgPropertiesCp.setValue(String.format("action_%s.properties", an.getName()));
		pl.add(cfgPropertiesCp);
		return mr;
	}
	
	private static JAVA genJavaAction(Flow flow, ActionNode an, boolean hasInstanceId){
		JAVA ja = new JAVA();
		ja.setJobTracker(jobTrackValue);
		ja.setNameNode(nameNodeValue);
		ja.setMainClass(EngineConf.etlcmd_main_class);
		ja.getArg().add((String) an.getProperty(ActionNode.key_cmd_class));
		ja.getArg().add(flow.getName());
		if (!hasInstanceId){
			ja.getArg().add(wfid);	
		}else{
			ja.getArg().add(wfid_param_exp);
		}
		ja.getArg().add(String.format("action_%s.properties", an.getName()));
		ja.getArg().add(nameNodeValue);
		ja.getArg().addAll(an.getAddArgs());
		return ja;
	}
	
	private static FORK getForkNode(WORKFLOWAPP wfa, String nodeName){
		List<Object> lo = wfa.getDecisionOrForkOrJoin();
		for (Object o: lo){
			if (o instanceof FORK){
				FORK f = (FORK)o;
				if (f.getName().equals(nodeName)){
					return f;
				}
			}
		}
		return null;
	}
	
	private static ACTION getActionNode(WORKFLOWAPP wfa, String nodeName){
		List<Object> lo = wfa.getDecisionOrForkOrJoin();
		for (Object o: lo){
			if (o instanceof ACTION){
				ACTION a = (ACTION)o;
				if (a.getName().equals(nodeName)){
					return a;
				}
			}
		}
		return null;
	}
	
	private static JOIN getJoinNode(WORKFLOWAPP wfa, String nodeName){
		List<Object> lo = wfa.getDecisionOrForkOrJoin();
		for (Object o: lo){
			if (o instanceof JOIN){
				JOIN j = (JOIN)o;
				if (j.getName().equals(nodeName)){
					return j;
				}
			}
		}
		return null;
	}
	
	private static String getJoinNodeName(String toNodeName){
		return String.format("%s_join", toNodeName);
	}
	
	private static String getForkNodeName(String fromNodeName){
		return String.format("%s_fork", fromNodeName);
	}
	
	//from node is generated
	private static boolean genLink(WORKFLOWAPP wfa, Flow flow, Link ln){
		Node toNode = flow.getNode(ln.getToNodeName());
		if (toNode==null){
			logger.error(String.format("to node %s not found.", ln.getToNodeName()));
			return false;
		}
		Node fromNode = flow.getNode(ln.getFromNodeName());
		if (fromNode==null){
			logger.error(String.format("from node %s not found.", ln.getFromNodeName()));
			return false;
		}
		boolean useFork = false;
		ACTION fromAction=null;
		FORK forkNode=null;
		if (flow.getOutLinks(fromNode.getName()).size()>1){//when fromNode has multiple out/next links, we need to generate a fork node
			useFork=true;
			String forkName = getForkNodeName(ln.getFromNodeName());
			forkNode = getForkNode(wfa, forkName);
			if (forkNode == null){
				logger.error(String.format("fork node:%s not found.", forkName));
				return false;
			}
		}else{
			fromAction = getActionNode(wfa, ln.getFromNodeName());
			if (fromAction == null){
				logger.error(String.format("action node:%s not found.", ln.getFromNodeName()));
				return false;
			}
		}
		String nextNodeName = ln.getToNodeName();
		if (flow.getInLinks(toNode.getName()).size()>1){//when toNode has multiple in links, we need to generate a join node
			//may need to gen join, the toNode's corresponding join node is named as toNode.name+"_"+join
			String joinNodeName = getJoinNodeName(ln.getToNodeName());
			JOIN j = getJoinNode(wfa, joinNodeName);
			if (j==null){
				//gen join node
				j = new JOIN();
				j.setName(joinNodeName);
				j.setTo(toNode.getName());
				wfa.getDecisionOrForkOrJoin().add(j);
			}
			nextNodeName = j.getName();
		}
		//link to the nextNodeName
		if (useFork){
			FORKTRANSITION ft = new FORKTRANSITION();
			ft.setStart(nextNodeName);
			forkNode.getPath().add(ft);
		}else{
			ACTIONTRANSITION at = new ACTIONTRANSITION();
			at.setTo(nextNodeName);
			fromAction.setOk(at);
			fromAction.setError(errorTransition);
		}
		return true;
	}
	
	public static WORKFLOWAPP genWfXml(Flow flow){
		return genWfXml(flow, null, false);
	}
	
	public static WORKFLOWAPP genWfXml(Flow flow, String startNode, boolean hasInstanceId){
		flow.init();
		errorTransition.setTo(killName);
		//gen flow
		WORKFLOWAPP wfa = new WORKFLOWAPP();
		if (hasInstanceId){
			PARAMETERS.Property wfIdProperty = new PARAMETERS.Property();
			wfIdProperty.setName(wfid_param_name);
			wfa.getParameters().getProperty().add(wfIdProperty);
		}
		wfa.setName(flow.getName());
		StartNode start = flow.getStart();
		if (start==null){
			logger.error(String.format("wf %s does not have start node.", flow.getName()));
			return null;
		}
		START wfstart = new START();
		String startToNodeName = startNode;
		Node startToNode = null;
		if (startNode==null){
			Set<Link> links = flow.getOutLinks(start.getName());
			if (links==null || links.size()!=1){
				logger.error(String.format("start can only have one next. links:%s", links));
				return null;
			}
			Link link = links.iterator().next();
			startToNodeName = link.getToNodeName();
			startToNode = flow.getNode(link.getToNodeName());
		}else{
			startToNode = flow.getNode(startNode);
			if (startToNode==null){
				logger.error(String.format("startNode %s not found.", startNode));
				return null;
			}
		}
		wfstart.setTo(startToNodeName);
		wfa.setStart(wfstart);
		//gen kill
		KILL wfkill = new KILL();
		wfkill.setName(killName);
		wfkill.setMessage(killMessage);
		wfa.getDecisionOrForkOrJoin().add(wfkill);
		
		Set<Node> curNodes = new HashSet<Node>();
		curNodes.add(startToNode);
		Set<String> visited = new HashSet<String>();
		while(curNodes!=null && curNodes.size()>0){
			Set<Node> nextNodes = new HashSet<Node>();
			logger.info(String.format("cur nodes:%s", curNodes));
			for (Node node: curNodes){
				logger.info(String.format("visit node:%s", node));
				ACTION act = new ACTION();
				act.setName(node.getName());
				visited.add(node.getName());
				//gen node
				if (node instanceof ActionNode){
					ActionNode an = (ActionNode)node;
					ExeType exeType = ExeType.valueOf((String) an.getProperty(ActionNode.key_exe_type));
					if (exeType==ExeType.mr){
						act.setMapReduce(genMRAction(flow, an, hasInstanceId));
					}else if (exeType==ExeType.java){
						act.setJava(genJavaAction(flow, an, hasInstanceId));
					}
					wfa.getDecisionOrForkOrJoin().add(act);
				}else if (node instanceof CallSubFlowNode){
					CallSubFlowNode csfNode = (CallSubFlowNode)node;
					//${nameNode}/project1/flow1/flow1_workflow.xml
					String appPath = String.format("${nameNode}/%s/%s/%s_workflow.xml", csfNode.getPrjName(), 
							csfNode.getSubFlowName(), csfNode.getSubFlowName());
					SUBWORKFLOW subwf = new SUBWORKFLOW();
					act.setSubWorkflow(subwf);
					act.getSubWorkflow().setAppPath(appPath);
					act.getSubWorkflow().setPropagateConfiguration(new FLAG());
					wfa.getDecisionOrForkOrJoin().add(act);
				}else if (node instanceof EndNode){
					END wfend = new END();
					wfend.setName(EndNode.end_node_name);
					wfa.setEnd(wfend);
				}
				Set<Link> nls = flow.getOutLinks(node.getName());
				if (nls!=null){
					if (nls.size()>1){
						//gen fork node
						String forkNodeName = getForkNodeName(node.getName());
						FORK fork = new FORK();
						fork.setName(forkNodeName);
						wfa.getDecisionOrForkOrJoin().add(fork);
						//gen the transition to fork
						ACTIONTRANSITION at = new ACTIONTRANSITION();
						at.setTo(forkNodeName);
						act.setOk(at);
						act.setError(errorTransition);
					}
					//
					for (Link ln:nls){
						//gen link
						genLink(wfa, flow, ln);
						
						if (!visited.contains(ln.getToNodeName()) && !curNodes.contains(flow.getNode(ln.getToNodeName()))){
							logger.info(String.format("visited:%s, add toNode:%s", visited, ln.getToNodeName()));
							nextNodes.add(flow.getNode(ln.getToNodeName()));
						}
					}
				}
			}
			curNodes = nextNodes;
		}
		return wfa;
	}
	//2020-10-27T09:00Z
	public static final String key_app_path="workflowAppUri";
	public static COORDINATORAPP genCoordXml(Flow flow){
		//default startTime=now, endTime = 3*duration+startTime, timezone:currentTimezone
		int durationSec = flow.getStart().getDuration();
		ZoneId zoneId = ZoneId.systemDefault();
		ZonedDateTime now = ZonedDateTime.now(zoneId);
		ZonedDateTime later = now.plus(durationSec, ChronoUnit.SECONDS);
		return genCoordXml(flow, CoordConf.df.format(now), CoordConf.df.format(later), zoneId.getId());
	}
	
	public static COORDINATORAPP genCoordXml(Flow flow, String startTime, String endTime, String timezone){
		COORDINATORAPP coord = new COORDINATORAPP();
		coord.setStart(startTime);
		coord.setEnd(endTime);
		coord.setTimezone(timezone);
		etl.flow.oozie.coord.ACTION act = new etl.flow.oozie.coord.ACTION();
		coord.setAction(act);
		etl.flow.oozie.coord.WORKFLOW workflow = new etl.flow.oozie.coord.WORKFLOW();
		act.setWorkflow(workflow);
		workflow.setAppPath(String.format("${%s}", key_app_path));
		etl.flow.oozie.coord.CONFIGURATION conf = new etl.flow.oozie.coord.CONFIGURATION();
		workflow.setConfiguration(conf);
		{
			etl.flow.oozie.coord.CONFIGURATION.Property nameNodeProp = new etl.flow.oozie.coord.CONFIGURATION.Property();
			nameNodeProp.setName(OozieConf.key_nameNode);
			nameNodeProp.setValue(String.format("${%s}", OozieConf.key_nameNode));
			conf.getProperty().add(nameNodeProp);
		}{
			etl.flow.oozie.coord.CONFIGURATION.Property jobTrackerProp = new etl.flow.oozie.coord.CONFIGURATION.Property();
			jobTrackerProp.setName(OozieConf.key_jobTracker);
			jobTrackerProp.setValue(String.format("${%s}", OozieConf.key_jobTracker));
			conf.getProperty().add(jobTrackerProp);
		}{
			etl.flow.oozie.coord.CONFIGURATION.Property queueNameProp = new etl.flow.oozie.coord.CONFIGURATION.Property();
			queueNameProp.setName(OozieConf.key_queueName);
			queueNameProp.setValue(String.format("${%s}", OozieConf.key_queueName));
			conf.getProperty().add(queueNameProp);
		}
		return coord;
	}
}