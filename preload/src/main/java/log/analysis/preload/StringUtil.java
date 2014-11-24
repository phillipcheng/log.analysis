package log.analysis.preload;

public class StringUtil {
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
}
