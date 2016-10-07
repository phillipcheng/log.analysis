package etl.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
	
	private Map<String, String> properties;
	
	private Map<String, Node> nodes;

	private List<Link> links; //from action to action
	
	private Map<String, Data> dataMap;
	
	//cached structure
	private Map<String, Set<Link>> nodeLinkMap;
	private Map<String, Set<Node>> linkNodeMap;
	
	public Set<Link> getNextLinks(String nodeName){
		return nodeLinkMap.get(nodeName);
	}
	
	public Set<Node> getNextNodes(Link lnk){
		return linkNodeMap.get(lnk.getName());
	}
	
	public Node getNode(String name){
		return nodes.get(name);
	}
	
	public void init(){
		nodeLinkMap = new HashMap<String, Set<Link>>();
		for (Link lnk: links){
			Set<Link> ll = nodeLinkMap.get(lnk.getFromNodeName());
			if (ll == null){
				ll = new HashSet<Link>();
				nodeLinkMap.put(lnk.getFromNodeName(), ll);
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
