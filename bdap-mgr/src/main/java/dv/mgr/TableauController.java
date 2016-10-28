package dv.mgr;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import dv.tableau.bl.TableauBL;
import dv.tableau.bl.TableauBLImpl;
import dv.tableau.rest.ProjectType;
import dv.tableau.rest.TsResponse;
import dv.tableau.rest.WorkbookType;

/**
 * 
 * @author ximing
 *
 */
@RestController
@RequestMapping("/tableau/rest")
public class TableauController {
//	private  String username="admin";
//	private  String password="admin123";
	private  String username="";
	private  String password="";
	
	@Autowired
	private TableauBL tableauBL;
	
	@RequestMapping(value = "/signin",method = RequestMethod.GET)
	TsResponse signin(HttpServletRequest request){
		HttpSession session = request.getSession();
		Map map = (Map)session.getAttribute("userDetail");
		username = (String)map.get("name");
		password = (String)map.get("password");
		TsResponse response = tableauBL.signin(username, password, "");
		return response;
	}
	
	@RequestMapping(value = "/allProjects",method = RequestMethod.GET)
	List<ProjectType> allProjects(HttpServletRequest request){
		HttpSession session = request.getSession();
		Map map = (Map)session.getAttribute("userDetail");
		username = (String)map.get("name");
		password = (String)map.get("password");
		List<ProjectType> response = tableauBL.getProjects(username, password, "");
		return response;
	}
	@RequestMapping(value = "/allWorkbooks",method = RequestMethod.GET)
	List<WorkbookType> allWorkbooks(HttpServletRequest request){
		HttpSession session = request.getSession();
		Map map = (Map)session.getAttribute("userDetail");
		username = (String)map.get("name");
		password = (String)map.get("password");
		List<WorkbookType> response = tableauBL.getWorkbooksBySite(username, password, "");
		return response;
	}

}
