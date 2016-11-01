package etl.flow.test;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import bdap.util.EngineConf;
import bdap.util.HdfsUtil;
import bdap.util.PropertiesUtil;
import etl.flow.mgr.FileType;
import etl.flow.mgr.InMemFile;
import etl.flow.oozie.OozieConf;
import etl.flow.oozie.OozieFlowMgr;

public abstract class TestFlow {
	public static final Logger logger = LogManager.getLogger(TestFlow.class);
	
	private static String cfgProperties="testFlow.properties";
	
	private static String key_platform_src_root="platform.src.root";
	private static String key_localFolder="localFolder";
	private static String key_defaultFs="defaultFs";
	
	private PropertiesConfiguration pc;
	
	private String bdapSrcRoot="";
	private String localFolder = "";
	private FileSystem fs;
	private String defaultFS;
	private Configuration conf;
	
	//
	public static void setCfgProperties(String testProperties){
		cfgProperties = testProperties;
	}
	
	@Before
    public void setUp() {
		try{
			pc = PropertiesUtil.getPropertiesConfig(cfgProperties);
			localFolder = pc.getString(key_localFolder);
			conf = new Configuration();
			defaultFS = pc.getString(key_defaultFs);
			conf.set("fs.defaultFS", defaultFS);
			bdapSrcRoot = pc.getString(key_platform_src_root);
			if (defaultFS.contains("127.0.0.1")){
				fs = FileSystem.get(conf);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						fs = FileSystem.get(conf);
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
		oc.setOozieLibPath(String.format("%s/user/%s/share/lib/preload/lib/", oc.getNameNode(), oc.getUserName()));
		return oc;
	}
	
	public EngineConf getEC(){
		EngineConf ec = new EngineConf(cfgProperties);
		return ec;
	}
	
	private void deployEngine(OozieConf oc){
		FileSystem fs = HdfsUtil.getHadoopFs(oc.getNameNode());
		String[] libJars = new String[]{bdapSrcRoot+"bdap-common/target/bdap.common-0.1.0.jar", 
				bdapSrcRoot+"bdap-engine/target/bdap.engine-0.1.0.jar"};
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
	
	//action properties should prefix with action. mapping properties should suffix with mapping.properties
	public List<InMemFile> getDeploymentUnits(String folder, String[] jarPaths){
		List<InMemFile> fl = new ArrayList<InMemFile>();
		Path directoryPath = Paths.get(folder);
		try {
			if (Files.isDirectory(directoryPath)) {
				DirectoryStream<Path> stream = Files.newDirectoryStream(directoryPath, "action.*.properties");
				for (Path path : stream) {
				    logger.info(String.format("action path:%s", path.getFileName().toString()));
					byte[] content = Files.readAllBytes(path);
					fl.add(new InMemFile(FileType.actionProperty, path.getFileName().toString(), content));
				}
				
				stream = Files.newDirectoryStream(directoryPath, "*_mapping.properties");
				for (Path path : stream) {
				    logger.info(String.format("mapping path:%s", path.getFileName().toString()));
					byte[] content = Files.readAllBytes(path);
					fl.add(new InMemFile(FileType.ftmappingFile, path.getFileName().toString(), content));
				}
				
				stream = Files.newDirectoryStream(directoryPath, "*workflow.xml");
				for (Path path : stream) {
				    logger.info(String.format("workflow path:%s", path.getFileName().toString()));
					byte[] content = Files.readAllBytes(path);
					fl.add(new InMemFile(FileType.oozieWfXml, path.getFileName().toString(), content));
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
					Path jarFile = Paths.get(this.getLocalFolder() + jarPath);
					fl.add(new InMemFile(FileType.thirdpartyJar, jarFile.getFileName().toString(), Files.readAllBytes(jarFile)));
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return fl;
	}
	
	private String runFlow(String projectName, String flowName, String[] jars){
		OozieFlowMgr ofm = new OozieFlowMgr();
		List<InMemFile> deployFiles = getDeploymentUnits(String.format("%s/%s/%s", getLocalFolder(), projectName, flowName), jars);
		return ofm.deployAndRun(projectName, flowName, deployFiles, getOC(), getEC());
	}
	
	public String testFlow(String projectName, String flowName, String[] jars) {
		try {
			if (getEC().getDefaultFs().contains("127.0.0.1")){
				return runFlow(projectName, flowName, jars);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
				return ugi.doAs(new PrivilegedExceptionAction<String>() {
							public String run() throws Exception {
								return runFlow(projectName, flowName, jars);
							}
						});
			}
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public void runDeployBdap() {
		try {
			if (getEC().getDefaultFs().contains("127.0.0.1")){
				deployEngine(getOC());
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
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

	public FileSystem getFs() {
		return fs;
	}
	
	public String getDefaultFS() {
		return defaultFS;
	}
	
	public Configuration getConf(){
		return conf;
	}
	
	public String getLocalFolder() {
		return localFolder;
	}

	public void setLocalFolder(String localFolder) {
		this.localFolder = localFolder;
	}
}
