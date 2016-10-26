package etl.flow.mgr;

import java.io.File;
import java.util.ArrayList;
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
	public List<InMemFile> genProperties(Flow flow){
		List<InMemFile> propertyFiles = new ArrayList<InMemFile>();
		for (Node n: flow.getNodes()){
			if (n instanceof ActionNode){
				ActionNode an = (ActionNode) n;
				String propFileName = String.format("action_%s.properties", an.getName());
				byte[] bytes = PropertiesUtil.getPropertyFileContent(an.getUserProperties());
				propertyFiles.add(new InMemFile(FileType.actionProperty, propFileName, bytes));
			}
		}
		return propertyFiles;
	}
	
	public InMemFile genEnginePropertyFile(EngineConf ec){
		return new InMemFile(FileType.engineProperty, EngineConf.file_name, ec.getContent());
	}
	/**
	 * update helper jars
	 * @param files
	 * @param fsconf
	 * @param ec
	 * @return
	 */
	public abstract boolean uploadJars(InMemFile[] files, FlowServerConf fsconf, EngineConf ec);
	
	/**
	 * instance level execution, used more in testing the process before production deployment
	 * @param projectName
	 * @param flowName
	 * @param wfid
	 * @param startNode
	 * @return the wf instanceid
	 */
	public abstract String execute(String projectName, Flow flow, FlowServerConf fsconf, EngineConf ec, String startNode, String instanceId);
	
	/**
	 * deploy the process, it will be executed/instantiated by the start conditions
	 * for oozie, a cooridator is deployed
	 * for spark, a spark-application is submitted
	 * @param projectName
	 * @param flow
	 * @param fsconf: flow server conf
	 * @param thirdPartyJars: list of relative path of the thirdparty jars needed
	 * @param ec: the engine configuration like kafka server, db server, hdfs etc
	 * @return success or failure
	 */
	public abstract boolean deploy(String projectName, Flow flow, FlowServerConf fsconf, EngineConf ec);
}
