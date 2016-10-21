package dv.tableau.bl;

import dv.tableau.rest.TsResponse;

public interface TableauBL {
	public TsResponse signin(String username, String password);
	public TsResponse getProjects(String username, String password);
}
