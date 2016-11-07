package bdap.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PropertiesUtil {
	public static final Logger logger = LogManager.getLogger(PropertiesUtil.class);
	
	public static byte[] getPropertyFileContent(LinkedHashMap<String, String> props){
		StringBuffer sb = new StringBuffer();
		for (String key: props.keySet()){
			String str = String.format("%s=%s", key, props.get(key));
			sb.append(str).append("\n");
		}
		return sb.toString().getBytes();
	}
	
	//using linkedhashmap to keep the insertion order
	public static void writePropertyFile(String filestring, LinkedHashMap<String, String> props){
		List<String> lines = new ArrayList<String>();
		for (String key: props.keySet()){
			String str = String.format("%s=%s", key, props.get(key));
			lines.add(str);
		}
		java.nio.file.Path file = Paths.get(filestring);
		try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			logger.error("", e);
		}
	}
	
	//ordered
	public static LinkedHashMap<String, String> getPropertiesExactMap(String conf){
		BufferedReader in = null;
		LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
		try {
			URL url = Thread.currentThread().getContextClassLoader().getResource(conf);
			in = new BufferedReader(new InputStreamReader(url.openStream()));
			String l;
		    while ((l = in.readLine()) != null){
		    	if (l.contains("=")){
		    		String[] kv = l.split("=", 2);
		    		map.put(kv[0], kv[1]);
		    	}
		    }
		}catch(Exception e){
			logger.error(String.format("try to load properties form conf file: %s", conf), e);
		}finally{
			if (in!=null){
				try {
					in.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
		return map;
	}
	
	public static PropertiesConfiguration getPropertiesConfig(String conf){
		PropertiesConfiguration pc = null;
		try {
			URL url = Thread.currentThread().getContextClassLoader().getResource(conf);
			pc = new PropertiesConfiguration(url);
		} catch (ConfigurationException e) {
			File f = new File(conf);
			try {
				pc = new PropertiesConfiguration(f);
			}catch(Exception e1){
				logger.error("", e1);
			}
		}
		return pc;
	}
}
