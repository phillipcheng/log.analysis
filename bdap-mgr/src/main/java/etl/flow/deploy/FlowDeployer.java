package etl.flow.deploy;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import bdap.util.EngineConf;
import bdap.util.FileType;
import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import bdap.util.PropertiesUtil;
import bdap.util.SystemUtil;
import etl.flow.CoordConf;
import etl.flow.Flow;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;
import etl.flow.spark.SparkFlowMgr;
import etl.flow.spark.SparkServerConf;

public class FlowDeployer {

	public static final Logger logger = LogManager.getLogger(FlowDeployer.class);
	
	private static String cfgProperties="testFlow.properties";
	
	private static String key_platform_local_dist="platform.local.dist"; //all cmd required runtime and compile time libary
	private static String key_platform_remote_lib="platform.remote.lib";
	private static String key_platform_coordinator_xml="platform.coordinate.xml";
	private static String key_projects="projects";
	private static String key_local_dir="local.dir";
	private static String key_hdfs_dir="hdfs.dir";
	
	private static String key_hdfs_user="hdfs.user";
	private static String key_defaultFs="defaultFs";
	
	private PropertiesConfiguration pc;
	
	private String platformLocalDist;
	private String platformRemoteLib="";
	private String platformCoordinateXml="";
	private Map<String, String> projectLocalDirMap= new HashMap<String, String>();
	private Map<String, String> projectHdfsDirMap= new HashMap<String, String>();
	
	private FileSystem fs;
	private String defaultFS;
	private String hdfsUser;
	private boolean localDeploy=true;
	private transient Configuration conf;
	
	public FlowDeployer(){
		this(cfgProperties);
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
		conf = new Configuration();
		defaultFS = pc.getString(key_defaultFs);
		conf.set("fs.defaultFS", defaultFS);
		platformLocalDist = pc.getString(key_platform_local_dist);
		platformRemoteLib = pc.getString(key_platform_remote_lib);
		setPlatformCoordinateXml(pc.getString(key_platform_coordinator_xml));
		hdfsUser = pc.getString(key_hdfs_user);
		Set<String> ipAddresses = SystemUtil.getMyIpAddresses();
		try {
			if (ipAddresses.contains(defaultFS)){
				fs = FileSystem.get(conf);
				localDeploy = true;
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						fs = FileSystem.get(conf);
						localDeploy = false;
						return null;
					}
				});
			}
		}catch(Exception e){
			logger.error("", e);
		}
    }
    
    public String getProjectHdfsDir(String prjName){
    	return projectHdfsDirMap.get(prjName);
    }
    
    private void installPlatformLib(DirectoryStream<Path> stream) throws Exception{
    	for (Path path : stream) {
		    logger.info(String.format("lib path:%s", path.getFileName().toString()));
			byte[] content = Files.readAllBytes(path);
			String remotePath = String.format("%s/%s", platformRemoteLib, path.getFileName().toString());
			HdfsUtil.writeDfsFile(fs, remotePath, content);
		}
    }
    
    public void installEngine(boolean clean) throws Exception {
    	DirectoryStream<Path> stream = null;
    	if (clean){
    		fs.delete(new org.apache.hadoop.fs.Path(platformRemoteLib), true);
    	}
    	//copy lib
    	if (clean){
    		stream = Files.newDirectoryStream(Paths.get(String.format("%s%s", platformLocalDist, File.separator+"lib")), "*.jar");
    	}else{
    		stream = Files.newDirectoryStream(Paths.get(String.format("%s%s", platformLocalDist, File.separator+"lib")), "bdap*.jar");
    	}
    	installPlatformLib(stream);
    	//copy conf
    	byte[] coordinateXmlContent = Files.readAllBytes(Paths.get(String.format("%s%s", platformLocalDist, "\\cfg\\coordinator.xml")));
    	HdfsUtil.writeDfsFile(fs, platformCoordinateXml, coordinateXmlContent);
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
	
	public OozieConf getOC(){
		OozieConf oc = new OozieConf(cfgProperties);
		//set the platform lib path as the oozie lib path
		oc.setOozieLibPath(String.format("%s%s/", oc.getNameNode(), platformRemoteLib));
		oc.setUserName(hdfsUser);
		return oc;
	}
	
	public SparkServerConf getSSC(){
		SparkServerConf ssc = new SparkServerConf(cfgProperties);
		
		return ssc;
	}
	
	public EngineConf getEC(){
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
	
	private void deploy(String projectName, String flowName, String[] jars, boolean fromJson, boolean skipSchema, EngineType et) throws Exception{
		String localProjectFolder = this.projectLocalDirMap.get(projectName);
		String hdfsProjectFolder = this.projectHdfsDirMap.get(projectName);
		OozieFlowMgr ofm = new OozieFlowMgr();
		SparkFlowMgr sfm = new SparkFlowMgr();
		if (!fromJson){
			List<InMemFile> deployFiles = getDeploymentUnits(String.format("%s/%s", localProjectFolder, flowName), 
					jars, fromJson, skipSchema);
			logger.info(String.format("files deployed:%s", deployFiles));
			ofm.deployFlowFromXml(hdfsProjectFolder, flowName, deployFiles, getOC(), getEC());
		}else{
			String jsonFile = String.format("%s/%s/%s.json", localProjectFolder, flowName, flowName);
			Flow flow = (Flow) JsonUtil.fromLocalJsonFile(jsonFile, Flow.class);
			if (flow!=null){
				if (EngineType.oozie==et){
					ofm.deployFlowFromJson(projectName, flow, this, this.getOC(), this.getEC());
				}else{
					sfm.deployFlowFromJson(projectName, flow, this, this.getSSC(), this.getEC());
				}
			}
		}
	}
	
	public void runDeploy(String projectName, String flowName, String[] jars, boolean fromJson, boolean skipSchema, EngineType et) {
		try {
			if (localDeploy){		
				deploy(projectName, flowName, jars, fromJson, skipSchema, et);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
						public Void run() throws Exception {
							deploy(projectName, flowName, jars, fromJson, skipSchema, et);
							return null;
						}
					});
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public void runDeploy(String projectName, String flowName, String[] jars, boolean fromJson,EngineType et) {
		runDeploy(projectName, flowName, jars, fromJson, false, et);
	}
	
	private String execute(String projectName, String flowName){
		String hdfsProjectFolder = this.projectHdfsDirMap.get(projectName);
		OozieFlowMgr ofm = new OozieFlowMgr();
		return ofm.executeFlow(hdfsProjectFolder, flowName, getOC(), getEC());
	}
	
	public String runExecute(String projectName, String flowName) {
		try {
			if (localDeploy){		
				return execute(projectName, flowName);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
				return ugi.doAs(new PrivilegedExceptionAction<String>() {
						public String run() throws Exception {
							return execute(projectName, flowName);
						}
					});
			}
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	private String startCoordinator(String projectName, String flowName, CoordConf cc){
		String hdfsProjectFolder = this.projectHdfsDirMap.get(projectName);
		OozieFlowMgr ofm = new OozieFlowMgr();
		return ofm.executeCoordinator(hdfsProjectFolder, flowName, getOC(), getEC(), cc);
	}
	
	public String runStartCoordinator(String projectName, String flowName, CoordConf cc) {
		try {
			if (localDeploy){		
				return startCoordinator(projectName, flowName, cc);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
				return ugi.doAs(new PrivilegedExceptionAction<String>() {
						public String run() throws Exception {
							return startCoordinator(projectName, flowName, cc);
						}
					});
			}
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
		String[] jars=null;
		if (DeployCmd.deployFlow.toString().equals(cmd)){
			String engineType = args[3];
			if (args.length>4){
				jars = args[4].split(",");
			}
			fd.runDeploy(prjName, flowName, jars, false, EngineType.valueOf(engineType));
		}else if (DeployCmd.runFlow.toString().equals(cmd)){
			fd.runExecute(prjName, flowName);
		}else if (DeployCmd.runCoordinator.toString().equals(cmd)){
			int startIdx=3;
			String startTime=args[startIdx++];
			String endTime=args[startIdx++];
			String duration=args[startIdx++];
			CoordConf cc = new CoordConf(startTime, endTime, duration);
			cc.setCoordPath(fd.getPlatformCoordinateXml());
			fd.runStartCoordinator(prjName, flowName, cc);
		}
	}
	
	public FileSystem getFs() {
		return fs;
	}
	
	public String getDefaultFS() {
		return defaultFS;
	}
	
	public Configuration getConf(){
		return conf;
	}
	public String getPlatformCoordinateXml() {
		return platformCoordinateXml;
	}
	public void setPlatformCoordinateXml(String platformCoordinateXml) {
		this.platformCoordinateXml = platformCoordinateXml;
	}
	public String getPlatformLocalDist() {
		return platformLocalDist;
	}
	public void setPlatformLocalDist(String platformLocalDist) {
		this.platformLocalDist = platformLocalDist;
	}
}
