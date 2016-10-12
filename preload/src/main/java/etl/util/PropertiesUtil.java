package etl.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.EngineUtil;

public class PropertiesUtil {
	public static final Logger logger = LogManager.getLogger(PropertiesUtil.class);
	//using linkedhashmap to keep the insertion order
	public static void writePropertyFile(String filestring, Map<String, String> props){
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
	
	public static PropertiesConfiguration getPropertiesConfigFromDfs(FileSystem fs, String conf){
		BufferedReader br = null;
		try {
			PropertiesConfiguration pc = new PropertiesConfiguration();
			if (conf!=null){
				Path ip = new Path(conf);
		        br=new BufferedReader(new InputStreamReader(fs.open(ip)));
		        pc.load(br);
			}
	        return pc;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}finally{
			if (br!=null){
				try{
					br.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public static PropertiesConfiguration getMergedPCFromDfs(FileSystem fs, String conf){
		PropertiesConfiguration cmdpc = getPropertiesConfigFromDfs(fs, conf);
		PropertiesConfiguration enginepc = EngineUtil.getInstance().getEngineProp();
		Iterator<String> it = enginepc.getKeys();
		while (it.hasNext()){
			String key = it.next();
			cmdpc.addProperty(key, enginepc.getProperty(key));
		}
		
		return cmdpc;
	}
}
