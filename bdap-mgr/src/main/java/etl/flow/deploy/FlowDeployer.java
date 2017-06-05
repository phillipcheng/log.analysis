package etl.flow.deploy;

import java.io.File;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
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
import etl.engine.ETLCmd;
import etl.engine.types.InputFormatType;
import etl.engine.types.OutputFormat;
import etl.flow.ActionNode;
import etl.flow.CoordConf;
import etl.flow.Flow;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.spark.SparkFlowMgr;
import etl.flow.spark.SparkServerConf;

public class FlowDeployer {

	public static final Logger logger = LogManager.getLogger(FlowDeployer.class);
	public static final String defaultCfgProperties = "testFlow.properties";
	
	public static final String prop_outputformat_textfile="org.apache.hadoop.mapreduce.lib.output.TextOutputFormat";
	public static final String prop_outputformat_parquetfile="etl.output.ParquetOutputFormat";
	
	public static final String coordinator_xml="coordinator.xml";
	public static final String spark_wfxml="sparkcmd_workflow.xml";
	public static final String submitspark_shell="submitspark.sh";
	
	private static final String key_platform_local_dist="platform.local.dist";
	private static final String key_platform_remote_dist="platform.remote.dist";
	private static final String key_projects="projects";
	
	private static final String key_hdfs_user="hdfs.user";
	private static final String key_defaultFs="defaultFs";
	private static final String key_jobTracker="jobTracker";
	private static final String key_deploy_method="deploy.method";
	
	private String cfgProperties=null;
	private Configuration pc;
	
	/* Optional project service to access project configuration */
	private transient ProjectService projectService;
	
	private String platformLocalDist;
	private String platformRemoteDist;
	
	private DeployMethod deployMethod;
	private String defaultFS;
	private String hdfsUser;

	private transient org.apache.hadoop.conf.Configuration conf;
	
	public FlowDeployer(){
		this(defaultCfgProperties);
		this.cfgProperties = defaultCfgProperties;
	}
	
    public FlowDeployer(String properties) {
    	this(properties, null);
    }
	
    public FlowDeployer(String properties, ProjectService ps) {
    	if (properties!=null){
    		cfgProperties = properties;
    	}
		pc = PropertiesUtil.getPropertiesConfig(properties);
		
		if (ps != null)
			projectService = ps;
		else
			projectService = new DefaultProjectService(pc.getStringArray(key_projects), pc);

		conf = new org.apache.hadoop.conf.Configuration();
		defaultFS = pc.getString(key_defaultFs);
		conf.set("fs.defaultFS", defaultFS);
		if (pc.containsKey(key_jobTracker)) {
			String jobTracker = pc.getString(key_jobTracker,"127.0.0.1:8032");
			String[] t = jobTracker.split(":");
			if (t != null && t.length > 0)
				conf.set("yarn.resourcemanager.hostname", t[0]);
			conf.set("yarn.resourcemanager.address", jobTracker);
		}
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
    
    
    public static String getOutputFormat(ActionNode an){
    	Map<String, Object> sysProperties = an.getSysProperties();
    	OutputFormat of = OutputFormat.text;
    	if (sysProperties.containsKey(ETLCmd.cfgkey_output_file_format)){
    		of = OutputFormat.valueOf((String) sysProperties.get(ETLCmd.cfgkey_output_file_format));
    		if (OutputFormat.parquet==of){
    			return prop_outputformat_parquetfile;
    		}else{
    			return prop_outputformat_textfile;
    		}
    	}else{
			return prop_outputformat_textfile;
    	}
	}
    
	public String getProjectHdfsDir(String prjName){
    	return projectService.getHdfsDir(prjName);
    }
	
	public String getProjectLocalDir(String prjName){
    	return projectService.getLocalDir(prjName);
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
    	return projectService.listProjects();
    }
    
    public Collection<String> listFlows(String project){
    	String locaDir = projectService.getLocalDir(project);
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
		return ssc;
	}
	
	public EngineConf getEngineConfig(){
		EngineConf ec = new EngineConf(cfgProperties);
		return ec;
	}
	
	
	//action properties should prefix with action. mapping properties should suffix with mapping.properties
	public static List<InMemFile> getJarDU(String folder, String[] jarPaths) throws Exception{
		List<InMemFile> fl = new ArrayList<InMemFile>();
		if (jarPaths!=null){
			for (String jarPath: jarPaths){
				Path jarFile = Paths.get(jarPath);
				fl.add(new InMemFile(FileType.thirdpartyJar, jarFile.getFileName().toString(), Files.readAllBytes(jarFile)));
			}
		}
		return fl;
	}
	
	public static List<InMemFile> getPropDU(String folder, String[] propFiles) throws Exception{
		List<InMemFile> fl = new ArrayList<InMemFile>();
		if (propFiles!=null){
			for (String propPath: propFiles){
				Path propFilePath = Paths.get(propPath);
				fl.add(new InMemFile(FileType.ftmappingFile, propFilePath.getFileName().toString(), Files.readAllBytes(propFilePath)));
			}
		}
		return fl;
	}
	
	public static List<InMemFile> getDeploymentUnits(String folder, boolean fromJson, boolean skipSchema) throws Exception{
		List<InMemFile> fl = new ArrayList<InMemFile>();
		Path directoryPath = Paths.get(folder);
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
		return fl;
	}
	
	public void deploy(String path, byte[] content) {
    	deployMethod.createFile(path, content);
	}
	
	public void deploy(String path, InputStream inputStream) {
    	deployMethod.createFile(path, inputStream);
	}
	
	public void copyFromLocalFile(String localPath, String remotePath) {
		deployMethod.copyFromLocalFile(localPath, remotePath);
	}
	
	public void copyFromLocalFile(boolean delSrc, boolean overwrite, String localPath, String remotePath) {
		deployMethod.copyFromLocalFile(delSrc, overwrite, localPath, remotePath);
	}
	
	public void delete(String path, boolean recursive) {
		deployMethod.delete(path, recursive);
	}
	
	public List<String> listFiles(String path) {
		return deployMethod.listFiles(path);
	}
	
	public List<String> readFiles(List<String> paths){
		List<String> ret = new ArrayList<String>();
		for (String path:paths){
			ret.addAll(readFile(path));
		}
		return ret;
	}
	
	public List<String> readFile(String path) {
		return deployMethod.readFile(path);
	}

	public boolean existsDir(String path) {
		return deployMethod.exists(path);
	}
	
	private void deploy(String projectName, String flowName, String[] jars, String[] propFiles, boolean fromJson, boolean skipSchema, EngineType et) throws Exception{
		String localProjectFolder = projectService.getLocalDir(projectName);
		String hdfsProjectFolder = projectService.getHdfsDir(projectName);
		String localFlowFolder = String.format("%s/%s", localProjectFolder, flowName);
		OozieFlowMgr ofm = new OozieFlowMgr(deployMethod);
		SparkFlowMgr sfm = new SparkFlowMgr();
		if (!fromJson){
			List<InMemFile> deployFiles = getDeploymentUnits(localFlowFolder, fromJson, skipSchema);
			logger.info(String.format("files deployed:%s", deployFiles));
			ofm.deployFlowFromXml(hdfsProjectFolder, flowName, deployFiles, this);
			//deploy jar files
			List<InMemFile> jarDU = getJarDU(localFlowFolder, jars);
			FlowMgr.uploadFiles(hdfsProjectFolder, flowName, jarDU, this);
		}else{
			String jsonFile = String.format("%s/%s/%s.json", localProjectFolder, flowName, flowName);
			Flow flow = (Flow) JsonUtil.fromLocalJsonFile(jsonFile, Flow.class);
			if (flow!=null){
				if (EngineType.oozie==et){
					ofm.deployFlow(projectName, flow, jars, propFiles, this);
				}else if (EngineType.spark==et){
					sfm.deployFlow(projectName, flow, jars, propFiles, this);
				}else{
					logger.error(String.format("engine type:%s not supported for deploy.",et));
				}
			}else{
				logger.error(String.format("json file %s not found!", jsonFile));
			}
		}
	}
	
	public void runDeploy(String projectName, String flowName, String[] jars, String[] propFiles, EngineType et) {
		try {
			deploy(projectName, flowName, jars, propFiles, true, false, et);
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	private String execute(String prjName, String flowName, EngineType et){
		if (et == EngineType.oozie){
			OozieFlowMgr ofm = new OozieFlowMgr(deployMethod);
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
			OozieFlowMgr ofm = new OozieFlowMgr(deployMethod);
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
		//String hdfsProjectFolder = projectService.getHdfsDir(projectName);
		OozieFlowMgr ofm = new OozieFlowMgr(deployMethod);
		return ofm.executeCoordinator(projectName, flowName, this, cc);
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
		OozieFlowMgr ofm = new OozieFlowMgr(deployMethod);
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
			fd.runDeploy(prjName, flowName, jars, null, EngineType.valueOf(engineType));
		}else if (DeployCmd.runFlow.toString().equals(cmd)){
			fd.runExecute(prjName, flowName, EngineType.valueOf(engineType));
		}else if (DeployCmd.runCoordinator.toString().equals(cmd)){
			int startIdx=3;
			String startTime=args[++startIdx];
			String endTime=args[++startIdx];
			String duration=args[++startIdx];
			CoordConf cc = new CoordConf(startTime, endTime, duration);
			cc.setCoordPath(String.format("%s/cfg/%s",fd.getPlatformRemoteDist(),FlowDeployer.coordinator_xml));
			fd.runStartCoordinator(prjName, flowName, cc);
		}
	}
	
	public String getDefaultFS() {
		return defaultFS;
	}

	public String getHdfsUser() {
		return hdfsUser;
	}
	
	public org.apache.hadoop.conf.Configuration getConf(){
		return conf;
	}
	
	public Configuration getPc() {
		return pc;
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

	public DeployMethod getDeployMethod() {
		return deployMethod;
	}

	public void setDeployMethod(DeployMethod deployMethod) {
		this.deployMethod = deployMethod;
	}
}
