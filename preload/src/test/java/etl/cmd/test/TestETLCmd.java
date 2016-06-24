package etl.cmd.test;

import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.Before;

public class TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestETLCmd.class);
	private static String cfgProperties="testETLCmd.properties";
	private static String key_localFolder="localFolder";
	private String localFolder = "";
	private Properties p = new Properties();
	
	@Before
    public void setUp() {
		try{
			InputStream input = this.getClass().getClassLoader().getResourceAsStream(cfgProperties);
			if (input!=null){
				p.load(input);
				localFolder = (p.getProperty(key_localFolder));
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
}
