package etl.flow.oozie;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.flow.Flow;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.oozie.wf.WORKFLOWAPP;

public class OozieFlowMgr extends FlowMgr{
	
	public static final Logger logger = LogManager.getLogger(OozieFlowMgr.class);

	@Override
	public boolean deploy(Flow flow, FlowServerConf fsconf) {
		OozieConf oc = (OozieConf)fsconf;
		String dir = "pdegen";
		Path path = Paths.get(dir);
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			logger.error("", e);
		}
		//gen workflow.xml
		WORKFLOWAPP wfa = OozieGenerator.genWfXml(flow);
		String pdeflowFile = dir + File.separator + "pde.workflow.xml";
		FlowJaxbUtil.marshal(wfa, pdeflowFile);
		//gen action.properties
		genProperties(flow, dir);
		//gen job.properties
		
		//deploy
		return true;
	}

	@Override
	public boolean execute(String flowName, String wfid, String startNode) {
		// TODO Auto-generated method stub
		return false;
	}

}
