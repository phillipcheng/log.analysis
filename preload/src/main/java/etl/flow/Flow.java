package etl.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Flow extends Node{
	public static final Logger logger = LogManager.getLogger(Flow.class);
	public static final String key_defaultFs="defaultFs";
	public static final String key_wfName="wfName";
	
	private StartNode start;
	private EndNode end;
	
	private Map<String, String> properties = new LinkedHashMap<String, String>();
	
	private Map<String, Node> nodes;

	private List<Link> links; //from action to action
	
	private Map<String, Data> dataMap;
	
	//cached structure
	private Map<String, Set<Link>> nodeOutLinkMap;
	private Map<String, Set<Link>> nodeInLinkMap;
	private Map<String, Set<Node>> linkNodeMap;
	
	public Set<Link> getInLinks(String nodeName){
		return nodeInLinkMap.get(nodeName);
	}
	
	public Set<Link> getOutLinks(String nodeName){
		return nodeOutLinkMap.get(nodeName);
	}
	
	public Set<Node> getNextNodes(Link lnk){
		return linkNodeMap.get(lnk.getName());
	}
	
	public Node getNode(String name){
		return nodes.get(name);
	}
	
	public Data getDataDef(String dsName){
		return dataMap.get(dsName);
	}
	
	public void init(){
		nodeOutLinkMap = new HashMap<String, Set<Link>>();
		for (Link lnk: links){
			Set<Link> ll = nodeOutLinkMap.get(lnk.getFromNodeName());
			if (ll == null){
				ll = new HashSet<Link>();
				nodeOutLinkMap.put(lnk.getFromNodeName(), ll);
			}
			ll.add(lnk);
		}
		nodeInLinkMap = new HashMap<String, Set<Link>>();
		for (Link lnk: links){
			Set<Link> ll = nodeInLinkMap.get(lnk.getToNodeName());
			if (ll == null){
				ll = new HashSet<Link>();
				nodeInLinkMap.put(lnk.getToNodeName(), ll);
			}
			ll.add(lnk);
		}
		linkNodeMap = new HashMap<String, Set<Node>>();
		for (Link lnk: links){
			Set<Node> nl = linkNodeMap.get(lnk.getName());
			if (nl == null){
				nl = new HashSet<Node>();
				linkNodeMap.put(lnk.getName(), nl);
			}
			if (nodes.containsKey(lnk.getToNodeName())){
				nl.add(nodes.get(lnk.getToNodeName()));
			}else{
				logger.error(String.format("lnk %s's target node does not exist.", lnk));
			}
		}
	}
	
	public Flow(String name) {
		super(name, 0, 0);
	}
	
	
	public StartNode getStart() {
		return start;
	}

	public void setStart(StartNode start) {
		this.start = start;
	}

	public Map<String, String> getProperties() {
		return properties;
	}
	
	public void put(String key, String value){
		properties.put(key, value);
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public Map<String, Node> getNodes() {
		return nodes;
	}

	public void setNodes(Map<String, Node> nodes) {
		this.nodes = nodes;
	}
	
	public List<Link> getLinks() {
		return links;
	}

	public void setLinks(List<Link> links) {
		this.links = links;
	}

	public Map<String, Data> getDataMap() {
		return dataMap;
	}

	public void setDataMap(Map<String, Data> dataMap) {
		this.dataMap = dataMap;
	}

	public EndNode getEnd() {
		return end;
	}

	public void setEnd(EndNode end) {
		this.end = end;
	}
}
