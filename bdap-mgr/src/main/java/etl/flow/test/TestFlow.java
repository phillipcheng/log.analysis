package etl.flow.test;

import java.security.PrivilegedExceptionAction;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;

import bdap.util.PropertiesUtil;

public abstract class TestFlow {
	public static final Logger logger = LogManager.getLogger(TestFlow.class);
	
	private static String cfgProperties="testFlow.properties";
	
	private static String key_localFolder="localFolder";
	private static String key_defaultFs="defaultFs";
	
	
	private PropertiesConfiguration pc;
	
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
	
	//override by sub test cases
	public abstract String getResourceSubFolder();
	
	public String getResourceFolder() {
		if (getResourceSubFolder()==null){
			return localFolder;
		}else
			return localFolder + getResourceSubFolder();
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
