package hpe.mtc;

import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class Util {
	public static final Logger logger = Logger.getLogger(GenCmd.class);
	
	//k1=v1,k2=v2 =>{{k1,v1},{k2,v2}}
	public static TreeMap<String, String> parseMapParams(String params){
		TreeMap<String, String> paramsMap = new TreeMap<String, String>();
		if (params==null){
			return paramsMap;
		}
		String[] strParams = params.split(",");
		for (String strParam:strParams){
			String[] kv = strParam.split("=");
			if (kv.length<2){
				logger.error(String.format("wrong param format: %s", params));
			}else{
				paramsMap.put(kv[0].trim(), kv[1].trim());
			}
		}
		return paramsMap;
	}
}
