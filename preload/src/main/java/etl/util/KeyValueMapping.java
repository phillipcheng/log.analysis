package etl.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class KeyValueMapping {
	public static final Logger logger = Logger.getLogger(KeyValueMapping.class);
	
	Map<String, String> mapping;
	
	public KeyValueMapping(String mappingFile, String keyKey, String valueKey){
		logger.info(String.format("mapping file:%s", mappingFile));
		PropertiesConfiguration pc = Util.getPropertiesConfig(mappingFile);
		String[] keys = pc.getStringArray(keyKey);
		String[] values = pc.getStringArray(valueKey);
		logger.info(String.format("keys:%s", Arrays.asList(keys)));
		logger.info(String.format("values:%s", Arrays.asList(values)));
		mapping = new HashMap<String, String>();
		for (int i=0; i<keys.length; i++){
			mapping.put(keys[i], values[i]);
		}
	}
	
	public String getValue(String key){
		return mapping.get(key);
	}

}
