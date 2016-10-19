package dv.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * @author ximing
 *
 */
public class ConfigManager {

	public static Map getProperties() {
		Map returnMap = new HashMap();
		String path = new ConfigManager().getClass().getResource("/").getPath();
		System.out.println(path);
		InputStream inputStream = new RequestUtil().getClass().getClassLoader()
				.getResourceAsStream("config/config.properties");
		Properties prop = new Properties();
		try {
			prop.load(inputStream);
			String proxyHost = prop.getProperty("proxyHost");
			String proxyPort = prop.getProperty("proxyPort");
			String username = prop.getProperty("username");
			String password = prop.getProperty("password");
			String tableauip = prop.getProperty("tableauip");
			returnMap.put("proxyHost", proxyHost);
			returnMap.put("proxyPort", proxyPort);
			returnMap.put("username", username);
			returnMap.put("password", password);
			returnMap.put("tableauip", tableauip);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return returnMap;
	}
}
