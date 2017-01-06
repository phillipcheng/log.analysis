package etl.flow.deploy;

import java.util.Collection;

public interface ProjectService {

	String getHdfsDir(String prjName);

	String getLocalDir(String prjName);

	Collection<String> listProjects();

}
