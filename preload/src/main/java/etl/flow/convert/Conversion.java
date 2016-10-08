package etl.flow.convert;

import java.io.File;
import java.util.Map;

import etl.flow.ActionNode;
import etl.flow.Flow;
import etl.flow.Node;
import etl.util.Util;

public class Conversion {
	
	//generate the properties files for all the cmd to initiate
	public static void genProperties(Flow flow, String dir){
		Map<String, Node> nmap = flow.getNodes();
		for (String name: nmap.keySet()){
			Node n = nmap.get(name);
			if (n instanceof ActionNode){
				ActionNode an = (ActionNode) n;
				String propFileString = String.format("%s%s%s_%s.properties", dir, File.separator, flow.getName(), an.getName());
				Util.writePropertyFile(propFileString, an.getUserProperties());
			}
		}
	}
	
	public static Flow fromJson(String json){
		return null;
	}
	
	public static String toJson(Flow flow){
		return null;
	}
}
