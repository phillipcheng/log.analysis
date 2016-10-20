package dv.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dv.tableau.bl.TableauBLImpl;
import etl.util.PropertiesUtil;

/**
 * 
 * @author ximing
 *
 */
public class ConfigManager {
	public static final Logger logger = LogManager.getLogger(ConfigManager.class);
	public static Map getProperties() {
		Map<String, String> returnMap = new HashMap<String, String>();
		logger.info(ConfigManager.class.getResource("/").getPath());;
		InputStream inputStream = new ConfigManager().getClass().getClassLoader()
				.getResourceAsStream("config/config.properties");
		Properties prop = new Properties();
		try {
			prop.load(inputStream);
			returnMap.put("proxyHost", prop.getProperty("proxyHost"));
			returnMap.put("proxyPort", prop.getProperty("proxyPort"));
			returnMap.put("tableauip", prop.getProperty("tableauip"));
		} catch (IOException e1) {
			logger.error("", e1);
		}
		return returnMap;
	}
}
