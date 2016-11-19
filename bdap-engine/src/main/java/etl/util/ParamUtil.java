package etl.util;

import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ParamUtil {
	public static final Logger logger = LogManager.getLogger(ParamUtil.class);
	
	public static final String kvSep = "=";
	public static final String paramSep = ",";
	//k1=v1,k2=v2 =>{{k1,v1},{k2,v2}}
	public static TreeMap<String, String> parseMapParams(String params){
		TreeMap<String, String> paramsMap = new TreeMap<String, String>();
		if (params==null){
			return paramsMap;
		}
		String[] strParams = params.split(paramSep);
		for (String strParam:strParams){
			String[] kv = strParam.split(kvSep);
			if (kv.length<2){
				logger.error(String.format("wrong param format: %s", params));
			}else{
				paramsMap.put(kv[0].trim(), kv[1].trim());
			}
		}
		return paramsMap;
	}
	
	public static String makeMapParams(Map<String, String> params){
		StringBuffer sb = new StringBuffer();
		for (String key: params.keySet()){
			String value = params.get(key);
			sb.append(key).append(kvSep).append(value);
			sb.append(paramSep);
		}
		return sb.toString();
	}
	
	public static String makeMapParams(String[] keys, String[] values){
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<keys.length; i++){
			String key = keys[i];
			String value = values[i];
			sb.append(key).append(kvSep).append(value);
			sb.append(paramSep);
		}
		return sb.toString();
	}
}
