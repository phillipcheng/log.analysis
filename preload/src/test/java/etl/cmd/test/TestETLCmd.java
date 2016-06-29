package etl.cmd.test;

import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Before;

public class TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestETLCmd.class);
	
	public static final String remoteUser = "dbadmin";
	private static String cfgProperties="testETLCmd.properties";
	private static String key_localFolder="localFolder";
	private static String key_defaultFs="defaultFS";
	
	private Properties p = new Properties();
	
	private String localFolder = "";
	private FileSystem fs;
	private String defaultFS;
	private Configuration conf;
	
	@Before
    public void setUp() {
		try{
			InputStream input = this.getClass().getClassLoader().getResourceAsStream(cfgProperties);
			if (input!=null){
				p.load(input);
				localFolder = (p.getProperty(key_localFolder));
				conf = new Configuration();
				defaultFS = p.getProperty(key_defaultFs);
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
			}else{
				logger.error(String.format("%s not found in classpath.", cfgProperties));
			}
		}catch(Exception e){
			logger.error("", e);
		}
    }

	public String getLocalFolder() {
		return localFolder;
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
}
