package etl.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GlobExpPathFilter extends Configured implements PathFilter{

	public static final Logger logger = LogManager.getLogger(GlobExpPathFilter.class);
	
	public static final String cfgkey_path_filters="path.filters";
	
	private Pattern[] pathRegExps;
	private String orgPathFilter;
	
	@Override
	public boolean accept(Path path) {
		if (pathRegExps==null){
			return true;
		}else{//path is like hdfs://xxx:xxx/abc/xxx
			String row = path.toString();
			if (row.startsWith("hdfs://")) {
				/* Locate from the root path */
				row = row.substring(7);
				row = row.substring(row.indexOf("/"));
			}
			for (int i=0; i<pathRegExps.length; i++){
				Pattern pattern = pathRegExps[i];
				Matcher m = pattern.matcher(row);
				if (m.matches()){
					logger.debug(String.format("path %s matched with %s", path, orgPathFilter));
					return true;
				}
			}
		}
		return false;
	}
	
	@Override
	public void setConf(Configuration conf){
		if (conf!=null){
			String filters = conf.get(cfgkey_path_filters);
			if (filters!=null){
				orgPathFilter = filters;
				String[] pathGlobExps = filters.split(",");
				pathRegExps = new Pattern[pathGlobExps.length];
				for (int i=0; i<pathGlobExps.length; i++){
					String pathGlobExp = pathGlobExps[i];
					String pathRegExp = StringUtil.convertGlobToRegEx(pathGlobExp);
					pathRegExps[i] = Pattern.compile(pathRegExp);
				}
			}
		}
	}

}
