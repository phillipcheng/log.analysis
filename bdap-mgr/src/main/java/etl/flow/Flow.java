package etl.flow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class Flow extends Node{
	public static final Logger logger = LogManager.getLogger(Flow.class);
	public static final String key_defaultFs="defaultFs";
	public static final String key_wfName="wfName";
	
	private Map<String, String> properties = new LinkedHashMap<String, String>();
	
	private List<Node> nodes;//data link can be induced by checking same data specified by one outlet and one inlet

	private List<Link> links; //action link,
	
	private List<Data> data;
	
	//cached structure
	private transient Map<String, Node> nodeMap;
	private transient Map<String, Data> dataMap;
	private transient Map<String, Set<Link>> nodeOutLinkMap;
	private transient Map<String, Set<Link>> nodeInLinkMap;
	private transient Map<String, Set<Node>> linkNodeMap;
	
	public Flow(){
	}
	
	public void init(){
		nodeMap = new HashMap<String, Node>();
		for (Node n: nodes){
			nodeMap.put(n.getName(), n);
		}
		dataMap = new HashMap<String, Data>();
		for (Data d: data){
			dataMap.put(d.getName(), d);
		}
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
			String lnkName = lnk.toString();
			Set<Node> nl = linkNodeMap.get(lnkName);
			if (nl == null){
				nl = new HashSet<Node>();
				linkNodeMap.put(lnkName, nl);
			}
			if (nodeMap.containsKey(lnk.getToNodeName())){
				nl.add(nodeMap.get(lnk.getToNodeName()));
			}else{
				logger.error(String.format("lnk %s's target node does not exist.", lnk));
			}
		}
	}
	
	@Override
	public boolean equals(Object that){
		if (!(that instanceof Flow)){
			return false;
		}
		Flow tf = (Flow)that;
		if (!tf.getProperties().equals(this.getProperties())){
			return false;
		}
		if (!tf.getNodes().equals(this.getNodes())){
			return false;
		}
		if (!tf.getLinks().equals(this.getLinks())){
			return false;
		}
		if (!tf.getData().equals(this.getData())){
			return false;
		}
		return true;
	}
	
	public Set<Link> getInLinks(String nodeName){
		return nodeInLinkMap.get(nodeName);
	}
	
	public Set<Link> getOutLinks(String nodeName){
		return nodeOutLinkMap.get(nodeName);
	}
	
	public Set<Node> getNextNodes(Link lnk){
		return linkNodeMap.get(lnk.toString());
	}
	
	public Node getNode(String name){
		return nodeMap.get(name);
	}
	
	public Data getDataDef(String dsName){
		return dataMap.get(dsName);
	}
	
	public Flow(String name) {
		super(name);
	}
	
	@JsonIgnore
	public StartNode getStart() {
		return (StartNode) nodeMap.get(StartNode.start_node_name);
	}
	
	@JsonAnyGetter
	public Map<String, String> getProperties() {
		return properties;
	}
	@JsonAnySetter
	public void putProperty(String key, String value){
		properties.put(key, value);
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public List<Node> getNodes() {
		return nodes;
	}

	public void setNodes(List<Node> nodes) {
		this.nodes = nodes;
	}
	
	public List<Link> getLinks() {
		return links;
	}

	public void setLinks(List<Link> links) {
		this.links = links;
	}

	public List<Data> getData() {
		return data;
	}

	public void setData(List<Data> data) {
		this.data = data;
	}

}
