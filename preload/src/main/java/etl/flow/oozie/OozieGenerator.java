package etl.flow.oozie;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.ETLCmdMain;
import etl.engine.InvokeMapper;
import etl.flow.ActionNode;
import etl.flow.Data;
import etl.flow.EndNode;
import etl.flow.ExeType;
import etl.flow.Flow;
import etl.flow.InputFormatType;
import etl.flow.Link;
import etl.flow.Node;
import etl.flow.StartNode;
import etl.flow.oozie.wf.ACTION;
import etl.flow.oozie.wf.ACTIONTRANSITION;
import etl.flow.oozie.wf.CONFIGURATION;
import etl.flow.oozie.wf.DELETE;
import etl.flow.oozie.wf.END;
import etl.flow.oozie.wf.FORK;
import etl.flow.oozie.wf.FORKTRANSITION;
import etl.flow.oozie.wf.JAVA;
import etl.flow.oozie.wf.JOIN;
import etl.flow.oozie.wf.KILL;
import etl.flow.oozie.wf.MAPREDUCE;
import etl.flow.oozie.wf.PREPARE;
import etl.flow.oozie.wf.START;
import etl.flow.oozie.wf.WORKFLOWAPP;

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
		public static final String prop_inputformat_line="org.apache.hadoop.mapreduce.lib.input.NLineInputFormat";
		public static final String prop_inputformat_filename="etl.util.FilenameInputFormat";
		public static final String prop_inputformat_textfile="org.apache.hadoop.mapreduce.lib.input.TextInputFormat";
	public static final String prop_inputdirs="mapreduce.input.fileinputformat.inputdir";
	public static final String prop_outputformat="mapreduce.job.outputformat.class";
		public static final String prop_outputformat_null="org.apache.hadoop.mapreduce.lib.output.NullOutputFormat";
		public static final String prop_outputformat_textfile="org.apache.hadoop.mapreduce.lib.output.TextOutputFormat";
	public static final String prop_outputdirs="mapreduce.output.fileoutputformat.outputdir";
	//command parameter are defined in the InvokerMapper
	
	private static String killName = "fail";
	private static String killMessage = "failed, error message[${wf:errorMessage(wf:lastErrorNode())}]";
	private static ACTIONTRANSITION errorTransition = new ACTIONTRANSITION();
	public static final String wfid = "${wf:id()}";
	
	private static String getInputFormat(InputFormatType ift){
		if (InputFormatType.line == ift){
			return prop_inputformat_line;
		}else if (InputFormatType.fileName == ift){
			return prop_inputformat_filename;
		}else if (InputFormatType.File == ift){
			return prop_inputformat_textfile;
		}else{
			logger.error(String.format("inputformat:%s not supported", ift));
			return null;
		}
	}
	
	private static ETLCmd getCmd(ActionNode an){
		String cmdClazz = an.getProperties().get(ActionNode.key_cmd_class);
		try {
			ETLCmd cmd = (ETLCmd) Class.forName(cmdClazz).newInstance();
			return cmd;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	private static MAPREDUCE genMRAction(Flow flow, ActionNode an){
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
		mapperClassCp.setValue(etl.engine.InvokeMapper.class.getName());
		pl.add(mapperClassCp);
		ETLCmd cmd = getCmd(an);
		if (cmd.hasReduce()){
			CONFIGURATION.Property reducerClassCp = new CONFIGURATION.Property();
			reducerClassCp.setName(prop_reduce_class);
			reducerClassCp.setValue(etl.engine.InvokeReducer.class.getName());
			pl.add(reducerClassCp);
		}else{
			CONFIGURATION.Property reducerNumCp = new CONFIGURATION.Property();
			reducerNumCp.setName(prop_reduce_num);
			reducerNumCp.setValue("0");
			pl.add(reducerNumCp);
		}
		//input and output configuration
		Set<Link> inlinks = flow.getInLinks(an.getName());
		List<String> inputDataDirs = new ArrayList<String>();
		for (Link ln: inlinks){
			Data d = flow.getDataDef(ln.getDataName());
			if (d==null){
				logger.error(String.format("data %s not found.", ln.getDataName()));
				return null;
			}
			inputDataDirs.add(d.getLocation()+wfid);
		}
			//input properties
		CONFIGURATION.Property inputFormatCp = new CONFIGURATION.Property();
		inputFormatCp.setName(prop_inputformat);
		inputFormatCp.setValue(getInputFormat(InputFormatType.valueOf(an.getProperty(ActionNode.key_input_format))));
		pl.add(inputFormatCp);
		CONFIGURATION.Property inputDirsCp = new CONFIGURATION.Property();
		inputDirsCp.setName(prop_inputdirs);
		inputDirsCp.setValue(String.join(",", inputDataDirs));
		pl.add(inputDirsCp);
			//output properties
		CONFIGURATION.Property outputFormatCp = new CONFIGURATION.Property();
		outputFormatCp.setName(prop_outputformat);
		CONFIGURATION.Property outputDirCp = new CONFIGURATION.Property();
		outputDirCp.setName(prop_outputformat);
		Set<Link> outlinks = flow.getOutLinks(an.getName());
		String outputDataDir=null;
		if (outlinks!=null && outlinks.size()==1){//only support 1 output datasource, >1 are treated as no output
			Data d = flow.getDataDef(outlinks.iterator().next().getDataName());
			if (d==null){
				logger.error(String.format("data not found:%s", outlinks.iterator().next().getDataName()));
				return null;
			}
			outputDataDir = d.getLocation()+wfid;
			outputFormatCp.setValue(prop_outputformat_textfile);
			outputDirCp.setValue(outputDataDir);
			pl.add(outputDirCp);
			DELETE del = new DELETE();
			del.setPath(outputDataDir);
			PREPARE prepare = new PREPARE();
			mr.setPrepare(prepare);
			mr.getPrepare().getDelete().add(del);
		}else{//
			outputFormatCp.setValue(prop_outputformat_null);
		}
		pl.add(outputFormatCp);
		//cmd configuration
		CONFIGURATION.Property cmdClassNameCp = new CONFIGURATION.Property();
		cmdClassNameCp.setName(InvokeMapper.cfgkey_cmdclassname);
		cmdClassNameCp.setValue(cmd.getClass().getName());
		pl.add(cmdClassNameCp);
		CONFIGURATION.Property wfNameCp = new CONFIGURATION.Property();
		wfNameCp.setName(InvokeMapper.cfgkey_wfName);
		wfNameCp.setValue(flow.getName());
		pl.add(wfNameCp);
		CONFIGURATION.Property wfIdCp = new CONFIGURATION.Property();
		wfIdCp.setName(InvokeMapper.cfgkey_wfid);
		wfIdCp.setValue(wfid);
		pl.add(wfIdCp);
		CONFIGURATION.Property cfgPropertiesCp = new CONFIGURATION.Property();
		cfgPropertiesCp.setName(InvokeMapper.cfgkey_staticconfigfile);
		cfgPropertiesCp.setValue(String.format("%s_%s.properties", flow.getName(), an.getName()));
		pl.add(cfgPropertiesCp);
		
		return mr;
	}
	
	private static JAVA genJavaAction(Flow flow, ActionNode an){
		JAVA ja = new JAVA();
		ja.setJobTracker(jobTrackValue);
		ja.setNameNode(nameNodeValue);
		ja.setMainClass(ETLCmdMain.class.getName());
		ja.getArg().add(an.getProperty(ActionNode.key_cmd_class));
		ja.getArg().add(flow.getName());
		ja.getArg().add(wfid);
		ja.getArg().add(flow.getName()+".properties");
		ja.getArg().add(nameNodeValue);
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
		FORK fromFork=null;
		if (fromNode.getOutletNum()>1){
			useFork=true;
			String forkName = getForkNodeName(ln.getFromNodeName());
			fromFork = getForkNode(wfa, forkName);
			if (fromFork == null){
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
		if (toNode.getInletNum()>1){
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
			fromFork.getPath().add(ft);
		}else{
			ACTIONTRANSITION at = new ACTIONTRANSITION();
			at.setTo(nextNodeName);
			fromAction.setOk(at);
			fromAction.setError(errorTransition);
		}
		return true;
	}
	
	public static WORKFLOWAPP genWfXml(Flow flow){
		flow.init();
		errorTransition.setTo(killName);
		//gen flow
		WORKFLOWAPP wfa = new WORKFLOWAPP();
		wfa.setName(flow.getName());
		StartNode start = flow.getStart();
		if (start==null){
			logger.error(String.format("wf %s does not have start node.", flow.getName()));
			return null;
		}
		Set<Link> links = flow.getOutLinks(start.getName());
		if (links==null || links.size()!=1){
			logger.error(String.format("start can only have one next. links:%s", links));
			return null;
		}
		START wfstart = new START();
		Link link = links.iterator().next();
		wfstart.setTo(link.getToNodeName());
		wfa.setStart(wfstart);
		//gen kill
		KILL wfkill = new KILL();
		wfkill.setName(killName);
		wfkill.setMessage(killMessage);
		wfa.getDecisionOrForkOrJoin().add(wfkill);
		
		Set<Node> curNodes = flow.getNextNodes(link);
		while(curNodes!=null && curNodes.size()>0){
			Set<Node> nextNodes = new HashSet<Node>();
			for (Node node: curNodes){
				ACTION act = new ACTION();
				act.setName(node.getName());
				//gen node
				if (node instanceof ActionNode){
					ActionNode an = (ActionNode)node;
					if (an.getExeType()==ExeType.mr){
						act.setMapReduce(genMRAction(flow, an));
					}else if (an.getExeType()==ExeType.java){
						act.setJava(genJavaAction(flow, an));
					}
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
					}
					//
					for (Link ln:nls){
						//gen link
						genLink(wfa, flow, ln);
						nextNodes.add(flow.getNode(ln.getToNodeName()));
					}
				}
			}
			curNodes = nextNodes;
		}
		return wfa;
	}
}
