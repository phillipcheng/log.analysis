package dv.tableau.bl;

import java.util.List;

import dv.tableau.rest.ProjectType;
import dv.tableau.rest.TsResponse;
import dv.tableau.rest.WorkbookType;

public interface TableauBL {
	public TsResponse signin(String username, String password, String siteName);
	public List<ProjectType> getProjects(String username, String password, String siteName);
	public List<WorkbookType> getWorkbooksBySite(String username, String password, String siteName);
}
