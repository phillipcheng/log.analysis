package etl.cmd.test;

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
			p.load(this.getClass().getResourceAsStream(cfgProperties));
			localFolder = (p.getProperty(key_localFolder));
		}catch(Exception e){
			logger.error("", e);
		}
    }

	public String getLocalFolder() {
		return localFolder;
	}
}
