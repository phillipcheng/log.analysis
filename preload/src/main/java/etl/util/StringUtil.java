package etl.util;

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
	
	public static String convertGlobToRegEx(String line) {
	    line = line.trim();
	    int strLen = line.length();
	    StringBuilder sb = new StringBuilder(strLen);
	    // Remove beginning and ending * globs because they're useless
	    boolean escaping = false;
	    int inCurlies = 0;
	    for (char currentChar : line.toCharArray()){
	        switch (currentChar){
	        case '*':
	            if (escaping)
	                sb.append("\\*");
	            else
	                sb.append(".*");
	            escaping = false;
	            break;
	        case '?':
	            if (escaping)
	                sb.append("\\?");
	            else
	                sb.append('.');
	            escaping = false;
	            break;
	        case '.':
	        case '(':
	        case ')':
	        case '+':
	        case '|':
	        case '^':
	        case '$':
	        case '@':
	        case '%':
	            sb.append('\\');
	            sb.append(currentChar);
	            escaping = false;
	            break;
	        case '\\':
	            if (escaping){
	                sb.append("\\\\");
	                escaping = false;
	            }else
	                escaping = true;
	            break;
	        case '{':
	            if (escaping){
	                sb.append("\\{");
	            }else{
	                sb.append('(');
	                inCurlies++;
	            }
	            escaping = false;
	            break;
	        case '}':
	            if (inCurlies > 0 && !escaping){
	                sb.append(')');
	                inCurlies--;
	            }else if (escaping){
	                sb.append("\\}");
	            }else{
	                sb.append("}");
	            }
	            escaping = false;
	            break;
	        case ',':
	            if (inCurlies > 0 && !escaping){
	                sb.append('|');
	            }
	            else if (escaping)
	                sb.append("\\,");
	            else
	                sb.append(",");
	            break;
	        default:
	            escaping = false;
	            sb.append(currentChar);
	        }
	    }
	    return sb.toString();
	}
	
	public static String replaceSpaces(String v){
		return v.replaceAll("\\s","");
	}
}
