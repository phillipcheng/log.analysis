package etl.flow.oozie.test;

import java.io.File;

import org.junit.Test;

import bdap.util.EngineConf;
import bdap.util.JsonUtil;
import etl.flow.Flow;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;

public class TestOozieFlow {
	
	public OozieConf getOC(){
		OozieConf oc = new OozieConf();
		oc.setNameNode("hdfs://192.85.247.104:19000");
		oc.setJobTracker("192.85.247.104:8032");
		oc.setQueueName("default");
		oc.setOozieLibPath("${nameNode}/user/${user.name}/preload/lib/");
		return oc;
	}
	
	@Test
	public void testFlow1(){
		String projectName = "project1";
		OozieFlowMgr ofm = new OozieFlowMgr();
		String flowFile = getResourceSubFolder()+"flow1.json";
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(flowFile, Flow.class);
		EngineConf ec = new EngineConf(getResourceSubFolder()+ EngineConf.file_name);
		ofm.deploy(projectName, flow, getOC(), ec);
	}

	public String getResourceSubFolder() {
		return "flow" + File.separator;
	}
}
