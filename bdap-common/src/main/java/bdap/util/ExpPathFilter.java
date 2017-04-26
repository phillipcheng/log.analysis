package bdap.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExpPathFilter {

	public static final Logger logger = LogManager.getLogger(ExpPathFilter.class);
	
	public static final String cfgkey_path_filters="path.filters";
	
	private Pattern[] pathRegExps;
	private String orgPathFilter;
	
	
	
	public boolean accept(String path) {
		String[] pathGlobExps = orgPathFilter.split(",");
		pathRegExps = new Pattern[pathGlobExps.length];
		for (int i=0; i<pathGlobExps.length; i++){
			String pathGlobExp = pathGlobExps[i];
			String pathRegExp = convertGlobToRegEx(pathGlobExp);
			pathRegExps[i] = Pattern.compile(pathRegExp);
		}
		if (pathRegExps==null){
			return true;
		}else{//path is like hdfs://xxx:xxx/abc/xxx
			if (path.startsWith("hdfs://")) {
				/* Locate from the root path */
				path = path.substring(7);
				path = path.substring(path.indexOf("/"));
			}
			for (int i=0; i<pathRegExps.length; i++){
				Pattern pattern = pathRegExps[i];
				Matcher m = pattern.matcher(path);
				if (m.matches()){
					logger.debug(String.format("path %s matched with %s", path, orgPathFilter));
					return true;
				}
			}
		}
		return false;
	}
	
	public ExpPathFilter(String orgPathFilter) {
		super();
		this.orgPathFilter = orgPathFilter;
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

	public String getOrgPathFilter() {
		return orgPathFilter;
	}

	public void setOrgPathFilter(String orgPathFilter) {
		this.orgPathFilter = orgPathFilter;
	}

}
