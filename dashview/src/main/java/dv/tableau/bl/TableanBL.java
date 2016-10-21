package dv.tableau.bl;

import java.util.Map;

import dv.tableau.rest.TsResponse;

public interface TableanBL {
	public TsResponse signin(String username, String password);
	public TsResponse getProjects(String username, String password);
}
