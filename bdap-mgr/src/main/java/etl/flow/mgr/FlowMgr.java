package etl.flow.mgr;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.EngineConf;
import bdap.util.PropertiesUtil;
import etl.flow.ActionNode;
import etl.flow.Flow;
import etl.flow.Node;

public abstract class FlowMgr {
	public static final Logger logger = LogManager.getLogger(FlowMgr.class);
	
	
	//generate the properties files for all the cmd to initiate
	public void genProperties(Flow flow, String dir){
		for (Node n: flow.getNodes()){
			if (n instanceof ActionNode){
				ActionNode an = (ActionNode) n;
				String propFileString = String.format("%s%s%s_action_%s.properties", dir, File.separator, flow.getName(), an.getName());
				PropertiesUtil.writePropertyFile(propFileString, an.getUserProperties());
			}
		}
	}
	
	/**
	 * 1. generate the workflows, the properties required by the actions, other assistance files
	 * 2. put these files in a engine dependent structure on the hdfs
	 * @param refRoot: the reference root directory for all file related operations
	 * @param projectName
	 * @param flow
	 * @param fsconf: flow server conf
	 * @param thirdPartyJars: list of relative path of the thirdparty jars needed
	 * @param ec: the engine configuration like kafka server, db server, hdfs etc
	 * @return success or failure
	 */
	public abstract boolean deploy(String refRoot, String projectName, Flow flow, FlowServerConf fsconf, List<String> thirdPartyJars, EngineConf ec);
	
	/**
	 * @param projectName
	 * @param flowName
	 * @param wfid
	 * @param startNode
	 * @return
	 */
	public abstract boolean execute(String projectName, String flowName, String wfid, String startNode);
	
}
