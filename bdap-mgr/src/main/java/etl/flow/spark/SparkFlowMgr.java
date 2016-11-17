package etl.flow.spark;

import bdap.util.EngineConf;
import etl.flow.CoordConf;
import etl.flow.Flow;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.mgr.InMemFile;
import etl.flow.mgr.NodeInfo;
import etl.flow.oozie.OozieConf;

public class SparkFlowMgr extends FlowMgr{

	@Override
	public boolean deployFlowFromJson(String projectName, Flow flow, FlowServerConf fsconf, EngineConf ec) {
		
		return false;
	}

	@Override
	public String executeFlow(String projectDir, String flowName, OozieConf oc, EngineConf ec) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String executeCoordinator(String projectDir, String flowName, OozieConf oc, EngineConf ec, CoordConf cc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void uploadFiles(String projectName, String flowName, InMemFile[] files, FlowServerConf fsconf,
			EngineConf ec) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getFlowLog(String projectName, FlowServerConf fsconf, String instanceId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNodeLog(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NodeInfo getNodeInfo(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] listNodeInputFiles(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] listNodeOutputFiles(String projectName, FlowServerConf fsconf, String instanceId, String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InMemFile getDFSFile(FlowServerConf fsconf, String filePath) {
		// TODO Auto-generated method stub
		return null;
	}

}
