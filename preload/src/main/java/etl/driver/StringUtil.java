package etl.driver;

import java.util.Map;

public class StringUtil {

	//replace "[param]" with value from params map, only support type string, for list type, should call this multiple times
	public static String fillParams(String input, Map<String, Object> params, 
			String prefix, String postfix){//TODO slow
		if (input.contains(prefix) && input.contains(postfix) && params!=null){
			for (String key: params.keySet()){
				Object value = params.get(key);
				String strValue = "null";
				if (value != null){
					strValue = value.toString();
				}
				input = input.replace(prefix + key + postfix, strValue);
			}
		}
		return input;
	}
	
	public static String getStringBetweenFirstPreFirstPost(String v, String pre, String post){
		int beginIndex=0;
		int endIndex = v.length();
		if (pre!=null && !"".equals(pre)){
			beginIndex = v.indexOf(pre);
		}
		if (post!=null && !"".equals(post)){
			endIndex = v.indexOf(post, beginIndex);
		}
		if (endIndex != -1 && beginIndex != -1){
			if (pre==null){
				return v.substring(0, endIndex);
			}else{
				return v.substring(beginIndex + pre.length(), endIndex);
			}
		}
		else
			return v;
	}
	
	public static String getStringBetweenFirstPreLastPost(String v, String pre, String post){
		int beginIndex = v.indexOf(pre);
		int endIndex = v.lastIndexOf(post);
		if (endIndex != -1 && beginIndex != -1)
			return v.substring(beginIndex + pre.length(), endIndex);
		else
			return v;
	}
	
	public static String getStringBetweenLastPreFirstPost(String v, String pre, String post){
		int beginIndex = v.lastIndexOf(pre);
		int endIndex = v.indexOf(post, beginIndex);
		if (endIndex != -1 && beginIndex != -1)
			return v.substring(beginIndex + pre.length(), endIndex);
		else
			return v;
	}
	
	public static String getStringBetweenLastPreLastPost(String v, String pre, String post){
		int beginIndex = v.lastIndexOf(pre);
		int endIndex = v.lastIndexOf(post);
		if (endIndex != -1 && beginIndex != -1)
			return v.substring(beginIndex + pre.length(), endIndex);
		else
			return v;
	}
}
