package etl.flow.deploy;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.configuration.Configuration;
import bdap.util.EngineConf;
import bdap.util.FileType;
import bdap.util.JsonUtil;
import bdap.util.PropertiesUtil;
import etl.flow.CoordConf;
import etl.flow.Flow;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.spark.SparkFlowMgr;
import etl.flow.spark.SparkServerConf;

public class FlowDeployer {

	public static final Logger logger = LogManager.getLogger(FlowDeployer.class);
	public static final String defaultCfgProperties = "testFlow.properties";
	
	public static String coordinator_xml="coordinator.xml";
	public static String spark_wfxml="sparkcmd_workflow.xml";
	public static String submitspark_shell="submitspark.sh";
	
	private static String key_platform_local_dist="platform.local.dist";
	private static String key_platform_remote_dist="platform.remote.dist";
	private static String key_projects="projects";
	private static String key_local_dir="local.dir";
	private static String key_hdfs_dir="hdfs.dir";
	
	private static String key_hdfs_user="hdfs.user";
	private static String key_defaultFs="defaultFs";
	private static String key_deploy_method="deploy.method";
	
	private String cfgProperties=null;
	private Configuration pc;
	
	private String platformLocalDist;
	private String platformRemoteDist;
	private Map<String, String> projectLocalDirMap= new HashMap<String, String>();
	private Map<String, String> projectHdfsDirMap= new HashMap<String, String>();
	
	private DeployMethod deployMethod;
	private String defaultFS;
	private String hdfsUser;
	private transient org.apache.hadoop.conf.Configuration conf;
	
	public FlowDeployer(){
		this(defaultCfgProperties);
		this.cfgProperties = defaultCfgProperties;
	}
    public FlowDeployer(String properties) {
    	if (properties!=null){
    		cfgProperties = properties;
    	}
		pc = PropertiesUtil.getPropertiesConfig(properties);
		String[] projects = pc.getStringArray(key_projects);
		for (String project:projects){
			projectLocalDirMap.put(project, pc.getString(project + "." + key_local_dir));
			projectHdfsDirMap.put(project, pc.getString(project + "." + key_hdfs_dir));
		}
		conf = new org.apache.hadoop.conf.Configuration();
		defaultFS = pc.getString(key_defaultFs);
		conf.set("fs.defaultFS", defaultFS);
		platformLocalDist = pc.getString(key_platform_local_dist);
		platformRemoteDist = pc.getString(key_platform_remote_dist);
		hdfsUser = pc.getString(key_hdfs_user);
		
		deployMethod = createDeployMethod(pc.getString(key_deploy_method), hdfsUser, defaultFS, pc);
    }
    
    private DeployMethod createDeployMethod(String deployMethod, String remoteUser, String defaultFs, Configuration pc) {
		if (DeployMethod.FTP.equals(deployMethod))
			return new FTPDeployMethod(pc);
		else if (DeployMethod.SSH.equals(deployMethod))
			return new SSHDeployMethod(pc);
		else
			return new DefaultDeployMethod(remoteUser, defaultFs);
	}
    
	public String getProjectHdfsDir(String prjName){
    	return projectHdfsDirMap.get(prjName);
    }
    
    private void installPlatformLib(DirectoryStream<Path> stream) throws Exception{
    	for (Path path : stream) {
		    logger.info(String.format("lib path:%s", path.getFileName().toString()));
			byte[] content = Files.readAllBytes(path);
			String remotePath = String.format("%s/lib/%s", platformRemoteDist, path.getFileName().toString());
			deployMethod.createFile(remotePath, content);
		}
    }
    
    public void installEngine(boolean clean) throws Exception {
    	DirectoryStream<Path> stream = null;
    	if (clean){
    		deployMethod.delete(String.format("%s/lib/", platformRemoteDist), true);
    	}
    	//copy lib
    	if (clean){
    		stream = Files.newDirectoryStream(Paths.get(String.format("%s%s", platformLocalDist, File.separator+"lib")), "*.jar");
    	}else{
    		stream = Files.newDirectoryStream(Paths.get(String.format("%s%s", platformLocalDist, File.separator+"lib")), "bdap*.jar");
    	}
    	installPlatformLib(stream);
    	deployMethod.createFile(String.format("%s/cfg/%s", platformRemoteDist, FlowDeployer.coordinator_xml),
    			Files.readAllBytes(Paths.get(String.format("%s/cfg/%s", platformLocalDist, FlowDeployer.coordinator_xml))));

    	deployMethod.createFile(String.format("%s/cfg/%s", platformRemoteDist, FlowDeployer.spark_wfxml), 
				Files.readAllBytes(Paths.get(String.format("%s/cfg/%s", platformLocalDist, FlowDeployer.spark_wfxml))));
		
    	deployMethod.createFile(String.format("%s/cfg/lib/%s", platformRemoteDist, FlowDeployer.submitspark_shell), 
				Files.readAllBytes(Paths.get(String.format("%s/cfg/%s", platformLocalDist, FlowDeployer.submitspark_shell))));
    }
    
    public Collection<String> listProjects(){
    	return projectLocalDirMap.keySet();
    }
    
    public Collection<String> listFlows(String project){
    	String locaDir = projectLocalDirMap.get(project);
    	File[] directories = new File(locaDir).listFiles(File::isDirectory);
    	List<String> ret =new ArrayList<String>();
    	for (File dir:directories){
    		ret.add(dir.getName());
    	}
    	return ret;
    }
	
	public OozieConf getOozieServerConf(){
		OozieConf oc = new OozieConf(cfgProperties);
		//set the platform lib path as the oozie lib path
		oc.setOozieLibPath(String.format("%s%s/lib", oc.getNameNode(), platformRemoteDist));
		oc.setUserName(hdfsUser);
		return oc;
	}
	
	public SparkServerConf getSparkServerConf(){
		SparkServerConf ssc = new SparkServerConf(cfgProperties);
		ssc.setOozieServerConf(getOozieServerConf());
		return ssc;
	}
	
	public EngineConf getEngineConfig(){
		EngineConf ec = new EngineConf(cfgProperties);
		return ec;
	}
	
	//action properties should prefix with action. mapping properties should suffix with mapping.properties
	private List<InMemFile> getDeploymentUnits(String folder, String[] jarPaths, boolean fromJson, boolean skipSchema){
		List<InMemFile> fl = new ArrayList<InMemFile>();
		Path directoryPath = Paths.get(folder);
		try {
			if (Files.isDirectory(directoryPath)) {
				DirectoryStream<Path> stream = null;
				if (!fromJson){
					stream = Files.newDirectoryStream(directoryPath, "action*.properties");
					for (Path path : stream) {
					    logger.info(String.format("action path:%s", path.getFileName().toString()));
						byte[] content = Files.readAllBytes(path);
						fl.add(new InMemFile(FileType.actionProperty, path.getFileName().toString(), content));
					}
					stream = Files.newDirectoryStream(directoryPath, "*workflow.xml");
					for (Path path : stream) {
					    logger.info(String.format("workflow path:%s", path.getFileName().toString()));
						byte[] content = Files.readAllBytes(path);
						fl.add(new InMemFile(FileType.oozieWfXml, path.getFileName().toString(), content));
					}
				}
				if (!skipSchema){
					stream = Files.newDirectoryStream(directoryPath, "*.schema");
					for (Path path : stream) {
					    logger.info(String.format("schema path:%s", path.getFileName().toString()));
						byte[] content = Files.readAllBytes(path);
						fl.add(new InMemFile(FileType.logicSchema, path.getFileName().toString(), content));
					}
				}
				stream = Files.newDirectoryStream(directoryPath, "*_mapping.properties");
				for (Path path : stream) {
				    logger.info(String.format("mapping path:%s", path.getFileName().toString()));
					byte[] content = Files.readAllBytes(path);
					fl.add(new InMemFile(FileType.ftmappingFile, path.getFileName().toString(), content));
				}
				stream = Files.newDirectoryStream(directoryPath, "log4j*");
				for (Path path : stream) {
				    logger.info(String.format("log4j path:%s", path.getFileName().toString()));
					byte[] content = Files.readAllBytes(path);
					fl.add(new InMemFile(FileType.log4j, path.getFileName().toString(), content));
				}
				
			}
			if (jarPaths!=null){
				for (String jarPath: jarPaths){
					Path jarFile = Paths.get(jarPath);
					fl.add(new InMemFile(FileType.thirdpartyJar, jarFile.getFileName().toString(), Files.readAllBytes(jarFile)));
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return fl;
	}
	
	public void deploy(String path, byte[] content) {
    	deployMethod.createFile(path, content);
	}
	
	public void copyFromLocalFile(String localPath, String remotePath) {
		deployMethod.copyFromLocalFile(localPath, remotePath);
	}
	
	public void delete(String path, boolean recursive) {
		deployMethod.delete(path, recursive);
	}
	
	public List<String> listFiles(String path) {
		return deployMethod.listFiles(path);
	}
	
	public List<String> readFile(String path) {
		return deployMethod.readFile(path);
	}
	
	private void deploy(String projectName, String flowName, String[] jars, boolean fromJson, boolean skipSchema, EngineType et) throws Exception{
		String localProjectFolder = this.projectLocalDirMap.get(projectName);
		String hdfsProjectFolder = this.projectHdfsDirMap.get(projectName);
		OozieFlowMgr ofm = new OozieFlowMgr();
		SparkFlowMgr sfm = new SparkFlowMgr();
		if (!fromJson){
			List<InMemFile> deployFiles = getDeploymentUnits(String.format("%s/%s", localProjectFolder, flowName), 
					jars, fromJson, skipSchema);
			logger.info(String.format("files deployed:%s", deployFiles));
			ofm.deployFlowFromXml(hdfsProjectFolder, flowName, deployFiles, getOozieServerConf(), getEngineConfig());
		}else{
			String jsonFile = String.format("%s/%s/%s.json", localProjectFolder, flowName, flowName);
			Flow flow = (Flow) JsonUtil.fromLocalJsonFile(jsonFile, Flow.class);
			if (flow!=null){
				if (EngineType.oozie==et){
					ofm.deployFlowFromJson(projectName, flow, this);
				}else if (EngineType.spark==et){
					sfm.deployFlowFromJson(projectName, flow, this);
				}else{
					logger.error(String.format("engine type:%s not supported for deploy.",et));
				}
			}
		}
	}
	
	public void runDeploy(String projectName, String flowName, String[] jars, boolean fromJson, boolean skipSchema, EngineType et) {
		try {
			deploy(projectName, flowName, jars, fromJson, skipSchema, et);
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public void runDeploy(String projectName, String flowName, String[] jars, boolean fromJson, EngineType et) {
		runDeploy(projectName, flowName, jars, fromJson, false, et);
	}
	
	private String execute(String prjName, String flowName, EngineType et){
		if (et == EngineType.oozie){
			OozieFlowMgr ofm = new OozieFlowMgr();
			return ofm.executeFlow(prjName, flowName, this);
		}else if (et == EngineType.spark){
			SparkFlowMgr sfm = new SparkFlowMgr();
			return sfm.executeFlow(prjName, flowName, this);
		}else{
			logger.error(String.format("unsupported engine type:%s", et));
			return null;
		}
	}
	
	private String execute(String prjName, String flowName, String wfId, EngineType et){
		if (et == EngineType.oozie){
			OozieFlowMgr ofm = new OozieFlowMgr();
			return ofm.executeFlow(prjName, flowName, this, wfId);
		}else{
			logger.error(String.format("unsupported engine type:%s", et));
			return null;
		}
	}
	
	public String runExecute(String projectName, String flowName, EngineType et) {
		try {
			return execute(projectName, flowName, et);
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public String runExecute(String projectName, String flowName, String wfId, EngineType et) {
		try {
			return execute(projectName, flowName, wfId, et);
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	private String startCoordinator(String projectName, String flowName, CoordConf cc){
		String hdfsProjectFolder = this.projectHdfsDirMap.get(projectName);
		OozieFlowMgr ofm = new OozieFlowMgr();
		return ofm.executeCoordinator(hdfsProjectFolder, flowName, this, cc);
	}
	
	public String runStartCoordinator(String projectName, String flowName, CoordConf cc) {
		try {
			return startCoordinator(projectName, flowName, cc);
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	//generate oozie xml from json file
	public String genOozieFlow(String jsonFile){
		OozieFlowMgr ofm = new OozieFlowMgr();
		Flow flow = (Flow) JsonUtil.fromLocalJsonFile(jsonFile, Flow.class);
		String flowXml = ofm.genWfXmlFile(flow);
		logger.info("\n" + flowXml);
		return flowXml;
	}

	public static void usage(FlowDeployer fd){
		System.out.println(String.format("%s%s", FlowDeployer.class.getName(), " deployFlow spark/oozie prjName flowName [jars]"));
		System.out.println(String.format("%s%s", FlowDeployer.class.getName(), " runFlow prjName flowName"));
		System.out.println(String.format("%s%s", FlowDeployer.class.getName(), " runCoordinator prjName flowName startTime endTime duration"));
		System.out.println(String.format("time format:%s", CoordConf.sdformat));
		System.out.println(String.format("duration unit: minute, min:5"));
		for (String prj: fd.listProjects()){
			Collection<String> flows = fd.listFlows(prj);
			System.out.println(String.format("%s:\n  %s", prj, String.join("\n  ", flows)));
		}
	}
	/**
	 * config testFlow.propertie then
	 * 1. deployFlow prjName flowName jars
	 * 2. runFlow prjName flowName
	 * 3. deployCoordinator prjName flowName
	 * 4. runCoordinator prjName flowName
	 * @param args
	 */
	public static void main(String[] args){
		FlowDeployer fd = new FlowDeployer();
		if (args.length<3){
			usage(fd);
			return;
		}
		String cmd=args[0];
		String prjName=args[1];
		String flowName=args[2];
		String engineType = args[3];
		String[] jars=null;
		if (DeployCmd.deployFlow.toString().equals(cmd)){
			if (args.length>4){
				jars = args[4].split(",");
			}
			fd.runDeploy(prjName, flowName, jars, false, EngineType.valueOf(engineType));
		}else if (DeployCmd.runFlow.toString().equals(cmd)){
			fd.runExecute(prjName, flowName, EngineType.valueOf(engineType));
		}else if (DeployCmd.runCoordinator.toString().equals(cmd)){
			int startIdx=3;
			String startTime=args[startIdx++];
			String endTime=args[startIdx++];
			String duration=args[startIdx++];
			CoordConf cc = new CoordConf(startTime, endTime, duration);
			cc.setCoordPath(String.format("%s/cfg/%s",fd.getPlatformRemoteDist(),FlowDeployer.coordinator_xml));
			fd.runStartCoordinator(prjName, flowName, cc);
		}
	}
	
	public String getDefaultFS() {
		return defaultFS;
	}
	
	public org.apache.hadoop.conf.Configuration getConf(){
		return conf;
	}
	public String getPlatformLocalDist() {
		return platformLocalDist;
	}
	public void setPlatformLocalDist(String platformLocalDist) {
		this.platformLocalDist = platformLocalDist;
	}
	public String getPlatformRemoteDist() {
		return platformRemoteDist;
	}
	public void setPlatformRemoteDist(String platformRemoteDist) {
		this.platformRemoteDist = platformRemoteDist;
	}
}
