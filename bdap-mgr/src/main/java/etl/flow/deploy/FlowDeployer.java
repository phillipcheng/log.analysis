package etl.flow.deploy;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
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
import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import bdap.util.PropertiesUtil;
import bdap.util.SystemUtil;
import etl.flow.CoordConf;
import etl.flow.Flow;
import etl.flow.mgr.FileType;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;

public class FlowDeployer {

	public static final Logger logger = LogManager.getLogger(FlowDeployer.class);
	
	private static String cfgProperties="testFlow.properties";
	
	private static String key_platform_local_lib="platform.local.lib";
	private static String key_platform_remote_lib="platform.remote.lib";
	private static String key_platform_coordinator_xml="platform.coordinate.xml";
	private static String key_projects="projects";
	private static String key_local_dir="local.dir";
	private static String key_hdfs_dir="hdfs.dir";
	
	private static String key_hdfs_user="hdfs.user";
	private static String key_defaultFs="defaultFs";
	
	private PropertiesConfiguration pc;
	
	private String platformLocalLib="";
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
		platformLocalLib = pc.getString(key_platform_local_lib);
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
	
	public OozieConf getOC(){
		OozieConf oc = new OozieConf(cfgProperties);
		//set the platform lib path as the oozie lib path
		oc.setOozieLibPath(String.format("%s%s/", oc.getNameNode(), platformRemoteLib));
		oc.setUserName(hdfsUser);
		return oc;
	}
	
	public EngineConf getEC(){
		EngineConf ec = new EngineConf(cfgProperties);
		return ec;
	}
	
	private void deployEngine(OozieConf oc){
		FileSystem fs = HdfsUtil.getHadoopFs(oc.getNameNode());
		String[] libJars = new String[]{platformLocalLib+"bdap-common/target/bdap.common-r0.2.0.jar", 
				platformLocalLib+"bdap-engine/target/bdap.engine-r0.2.0.jar"};
		try {
			for (String libJar: libJars){
				Path localJar = Paths.get(libJar);
				String fileName = localJar.getFileName().toString();
				String remoteJar = oc.getOozieLibPath() + fileName;
				HdfsUtil.writeDfsFile(fs, remoteJar, Files.readAllBytes(localJar));
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public void runDeployEngine() {
		try {
			if (localDeploy){
				deployEngine(getOC());
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						deployEngine(getOC());
						return null;
					}
				});
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	//action properties should prefix with action. mapping properties should suffix with mapping.properties
	private List<InMemFile> getDeploymentUnits(String folder, String[] jarPaths, boolean fromJson){
		List<InMemFile> fl = new ArrayList<InMemFile>();
		Path directoryPath = Paths.get(folder);
		try {
			if (Files.isDirectory(directoryPath)) {
				DirectoryStream<Path> stream = null;
				if (!fromJson){
					stream = Files.newDirectoryStream(directoryPath, "action.*.properties");
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
	
	private void deploy(String projectName, String flowName, String[] jars, boolean fromJson){
		String localProjectFolder = this.projectLocalDirMap.get(projectName);
		String hdfsProjectFolder = this.projectHdfsDirMap.get(projectName);
		OozieFlowMgr ofm = new OozieFlowMgr();
		if (!fromJson){
			List<InMemFile> deployFiles = getDeploymentUnits(String.format("%s/%s", localProjectFolder, flowName), 
					jars, fromJson);
			ofm.deployFlowFromXml(hdfsProjectFolder, flowName, deployFiles, getOC(), getEC());
		}else{
			String jsonFile = String.format("%s/%s/%s.json", localProjectFolder, flowName, flowName);
			Flow flow = (Flow) JsonUtil.fromLocalJsonFile(jsonFile, Flow.class);
			ofm.deployFlowFromJson(hdfsProjectFolder, flow, this.getOC(), this.getEC());
		}
	}
	
	public void runDeploy(String projectName, String flowName, String[] jars, boolean fromJson) {
		try {
			if (localDeploy){		
				deploy(projectName, flowName, jars, fromJson);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
						public Void run() throws Exception {
							deploy(projectName, flowName, jars, fromJson);
							return null;
						}
					});
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	private String execute(String projectName, String flowName, boolean fromJson){
		String hdfsProjectFolder = this.projectHdfsDirMap.get(projectName);
		OozieFlowMgr ofm = new OozieFlowMgr();
		return ofm.executeFlow(hdfsProjectFolder, flowName, getOC(), getEC());
	}
	
	public String runExecute(String projectName, String flowName, boolean fromJson) {
		try {
			if (localDeploy){		
				return execute(projectName, flowName, fromJson);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
				return ugi.doAs(new PrivilegedExceptionAction<String>() {
						public String run() throws Exception {
							return execute(projectName, flowName, fromJson);
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

	public static void usage(){
		System.out.println(String.format("%s%s", FlowDeployer.class.getName(), " (deploy/run)(Flow/Coordinator) prjName flowName [jars]"));
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
		if (args.length<3){
			usage();
			return;
		}
		String cmd=args[0];
		String prjName=args[1];
		String flowName=args[2];
		String[] jars=null;
		FlowDeployer fd = new FlowDeployer();
		if (DeployCmd.deployFlow.toString().equals(cmd)){
			if (args.length>3){
				jars = args[3].split(",");
			}
			fd.runDeploy(prjName, flowName, jars, false);
		}else if (DeployCmd.runFlow.toString().equals(cmd)){
			fd.runExecute(prjName, flowName, false);
		}else if (DeployCmd.runCoordinator.toString().equals(cmd)){
			int startIdx=3;
			String startTime=args[startIdx++];
			String endTime=args[startIdx++];
			int duration=Integer.parseInt(args[startIdx++]);
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
}
