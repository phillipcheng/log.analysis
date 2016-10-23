package etl.flow.spark;

import java.util.List;

import bdap.util.EngineConf;
import etl.flow.Flow;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;

public class SparkFlowMgr extends FlowMgr{

	@Override
	public boolean deploy(String refRoot, String projectName, Flow flow, FlowServerConf fsconf,
			List<String> thirdPartyJars, EngineConf ec) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute(String projectName, String flowName, String wfid, String startNode) {
		// TODO Auto-generated method stub
		return false;
	}

}
