package etl.flow.spark;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.EngineConf;
import bdap.util.SystemUtil;
import etl.flow.CoordConf;
import etl.flow.Flow;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.FlowServerConf;
import etl.flow.mgr.InMemFile;
import etl.flow.mgr.NodeInfo;

public class SparkFlowMgr extends FlowMgr{
	public static final Logger logger = LogManager.getLogger(FlowMgr.class);

	@Override
	public boolean deployFlowFromJson(String prjName, Flow flow, FlowDeployer fd, FlowServerConf fsconf, EngineConf ec) throws Exception{
		SparkServerConf ssc = (SparkServerConf) fsconf;
		//generate the driver java file
		String srcRootDir = String.format("%s/%s/%s", ssc.getTmpFolder(), prjName, ssc.getSrcFolder());
		if (!Files.exists(Paths.get(srcRootDir))) Files.createDirectories(Paths.get(srcRootDir));
		SparkGenerator.genDriverJava(prjName, flow, srcRootDir, ssc);
		//compile the file
		String classesRootDir = String.format("%s/%s/%s", ssc.getTmpFolder(), prjName, ssc.getClassesFolder());
		if (!Files.exists(Paths.get(classesRootDir))) Files.createDirectories(Paths.get(classesRootDir));
		String cpPath = String.format("%s/buildlib/*", fd.getPlatformLocalDist());
		String javacmd = String.format("%s/javac -cp \"%s\" -d %s %s/%s/%s.java", ssc.getJdkBin(), cpPath, 
				classesRootDir, srcRootDir, prjName, flow.getName());
		logger.info(javacmd);
		String output = SystemUtil.execCmd(javacmd);
		logger.info(output);
		//generate action properties files
		List<InMemFile> imfiles = new ArrayList<InMemFile>();
		imfiles.add(super.genEnginePropertyFile(ec));
		//generate etlengine.properties
		imfiles.addAll(super.genProperties(flow));
		//jar the file
		for (InMemFile im: imfiles){
			Path path = Paths.get(String.format("%s/%s", classesRootDir, im.getFileName()));
			Files.write(path, im.getContent());
		}
		//deploy
		return false;
	}

	@Override
	public String executeFlow(String projectDir, String flowName, FlowServerConf fsconf, EngineConf ec) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String executeCoordinator(String projectDir, String flowName, FlowServerConf fsconf, EngineConf ec,
			CoordConf cc) {
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
	public void executeAction(String projectName, Flow flow, List<InMemFile> imFiles, FlowServerConf fsconf,
			EngineConf ec, String actionNode, String instanceId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public FlowInfo getFlowInfo(String projectName, FlowServerConf fsconf, String instanceId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] listNodeInputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId,
			String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] listNodeOutputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId,
			String nodeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InMemFile getDFSFile(EngineConf ec, String filePath) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InMemFile getDFSFile(EngineConf ec, String filePath, int maxFileSize) {
		// TODO Auto-generated method stub
		return null;
	}

}
