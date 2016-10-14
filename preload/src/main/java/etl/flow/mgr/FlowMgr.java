package etl.flow.mgr;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.flow.ActionNode;
import etl.flow.Flow;
import etl.flow.Node;
import etl.util.PropertiesUtil;

public abstract class FlowMgr {
	public static final Logger logger = LogManager.getLogger(FlowMgr.class);
	
	//generate the properties files for all the cmd to initiate
	public void genProperties(Flow flow, String dir){
		for (Node n: flow.getNodes()){
			if (n instanceof ActionNode){
				ActionNode an = (ActionNode) n;
				String propFileString = String.format("%s%s%s_%s.properties", dir, File.separator, flow.getName(), an.getName());
				PropertiesUtil.writePropertyFile(propFileString, an.getUserProperties());
			}
		}
	}
	
	/**
	 * 1. generate the workflows, the properties required by the actions, other assistance files
	 * 2. put these files in a engine dependent structure on the hdfs
	 * @param flow
	 * @param fsconf
	 * @param startNode
	 * @return
	 */
	public abstract boolean deploy(Flow flow, FlowServerConf fsconf);
	
	/**
	 * @param flow:
	 * @param wfid
	 * @return
	 */
	public abstract boolean execute(String flowName, String wfid, String startNode);
	
}
