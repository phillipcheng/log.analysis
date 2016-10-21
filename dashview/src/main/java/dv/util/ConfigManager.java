package dv.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dv.tableau.bl.TableauBLImpl;
import etl.util.LocalPropertiesUtil;
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
		PropertiesConfiguration pc = LocalPropertiesUtil.getPropertiesConfig("config/config.properties");
		returnMap.put("proxyHost", pc.getString("proxyHost"));
		returnMap.put("proxyPort", pc.getString("proxyPort"));
		returnMap.put("tableauip", pc.getString("tableauip"));
		logger.info(returnMap);
		return returnMap; 
	}
}
