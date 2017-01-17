package etl.flow.deploy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

public class DefaultProjectService implements ProjectService {
	private final static String key_local_dir="local.dir";
	private final static String key_hdfs_dir="hdfs.dir";
	
	private Map<String, String> projectLocalDirMap= new HashMap<String, String>();
	private Map<String, String> projectHdfsDirMap= new HashMap<String, String>();

	public DefaultProjectService(String[] projects, Configuration pc) {
		if (projects != null)
			for (String project:projects){
				projectLocalDirMap.put(project, pc.getString(project + "." + key_local_dir));
				projectHdfsDirMap.put(project, pc.getString(project + "." + key_hdfs_dir));
			}
	}
	
	public String getHdfsDir(String prjName) {
		return projectHdfsDirMap.get(prjName);
	}
	
	public String getLocalDir(String prjName) {
		return projectLocalDirMap.get(prjName);
	}
	
	public Collection<String> listProjects() {
		return projectLocalDirMap.keySet();
	}
}
