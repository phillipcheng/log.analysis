package etl.flow.oozie;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.flow.Flow;
import etl.flow.convert.Conversion;
import etl.flow.oozie.wf.WORKFLOWAPP;
import etl.util.JaxbUtil;

public class OozieDeployer {
	
	public static final Logger logger = LogManager.getLogger(OozieDeployer.class);
	
	public static void deploy(Flow flow){
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
		JaxbUtil.marshal(wfa, pdeflowFile);
		//gen action.properties
		Conversion.genProperties(flow, dir);
		//gen job.properties
		//deploy
	}

}
