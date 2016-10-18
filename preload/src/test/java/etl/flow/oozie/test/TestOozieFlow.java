package etl.flow.oozie.test;

import org.junit.Test;

import etl.cmd.test.TestETLCmd;
import etl.flow.Flow;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;
import etl.util.JsonUtil;

public class TestOozieFlow extends TestETLCmd{
	
	@Test
	public void testFlow1(){
		OozieFlowMgr ofm = new OozieFlowMgr();
		OozieConf oc = new OozieConf();
		String flowFile = super.getLocalFolder()+"flow1.json";
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(flowFile, Flow.class);
		ofm.deploy(flow, oc);
		
	}

	@Override
	public String getResourceSubFolder() {
		return "flow1/";
	}

}
