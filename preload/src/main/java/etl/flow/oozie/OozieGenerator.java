package etl.flow.oozie;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.flow.ActionNode;
import etl.flow.EndNode;
import etl.flow.ExeType;
import etl.flow.Flow;
import etl.flow.Link;
import etl.flow.Node;
import etl.flow.StartNode;
import etl.flow.oozie.wf.ACTION;
import etl.flow.oozie.wf.ACTIONTRANSITION;
import etl.flow.oozie.wf.END;
import etl.flow.oozie.wf.FORK;
import etl.flow.oozie.wf.FORKTRANSITION;
import etl.flow.oozie.wf.JAVA;
import etl.flow.oozie.wf.JOIN;
import etl.flow.oozie.wf.KILL;
import etl.flow.oozie.wf.MAPREDUCE;
import etl.flow.oozie.wf.START;
import etl.flow.oozie.wf.WORKFLOWAPP;

public class OozieGenerator {
	public static final Logger logger = LogManager.getLogger(OozieGenerator.class);
	
	private static String killName = "fail";
	private static String killMessage = "failed, error message[${wf:errorMessage(wf:lastErrorNode())}]";
	private static ACTIONTRANSITION errorTransition = new ACTIONTRANSITION();
	
	private static MAPREDUCE genMRAction(ActionNode an){
		MAPREDUCE mr = new MAPREDUCE();
		return mr;
	}
	
	private static JAVA genJavaAction(ActionNode an){
		JAVA ja = new JAVA();
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
		
		StartNode start = flow.getStart();
		if (start==null){
			logger.error(String.format("wf %s does not have start node.", flow.getName()));
			return null;
		}
		Set<Link> links = flow.getNextLinks(start.getName());
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
						act.setMapReduce(genMRAction(an));
					}else if (an.getExeType()==ExeType.java){
						act.setJava(genJavaAction(an));
					}
					wfa.getDecisionOrForkOrJoin().add(act);
				}else if (node instanceof EndNode){
					END wfend = new END();
					wfend.setName(EndNode.end_node_name);
					wfa.setEnd(wfend);
				}
				Set<Link> nls = flow.getNextLinks(node.getName());
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
