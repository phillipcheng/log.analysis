package dv.mgr;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import dv.tableau.bl.TableauBLImpl;

/**
 * 
 * @author ximing
 *
 */
@RestController
@RequestMapping("/tableau/rest")
public class TableauController {
	
	@RequestMapping(value = "/signin",method = RequestMethod.GET)
	Map signin(){
		Map response = new TableauBLImpl().signin();
		return response;
	}
	
	@RequestMapping(value = "/allProjects",method = RequestMethod.GET)
	Map allProjects(){
		Map response = new TableauBLImpl().getProjects();
		return response;
	}

}
