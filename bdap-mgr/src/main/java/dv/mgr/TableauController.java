package dv.mgr;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import dv.tableau.bl.TableauBLImpl;
import dv.tableau.rest.TsResponse;

/**
 * 
 * @author ximing
 *
 */
@RestController
@RequestMapping("/tableau/rest")
public class TableauController {
	private String conf = "config/config.properties";
	private  String username="admin";
	private  String password="admin123";
	
	@RequestMapping(value = "/signin",method = RequestMethod.GET)
	TsResponse signin(){
		TsResponse response = new TableauBLImpl(conf).signin(username, password);
		return response;
	}
	
	@RequestMapping(value = "/allProjects",method = RequestMethod.GET)
	TsResponse allProjects(){
		TsResponse response = new TableauBLImpl(conf).getProjects(username, password);
		return response;
	}

}
