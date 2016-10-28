package dv.mgr;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import dv.db.entity.AccountEntity;
import dv.tableau.bl.CommonBLImpl;

@RestController
//@Controller
@RequestMapping("/login")
public class LoginController {
	@Autowired
	CommonBLImpl commonBL;
	
	@RequestMapping(value = "/signin",method = RequestMethod.POST)
    public String signin (@ModelAttribute AccountEntity account, Model model, HttpServletRequest request, HttpServletResponse response){
		boolean enableLogin = commonBL.validateLogin(account);
		ModelAndView modelView = new ModelAndView();
		if(enableLogin){
			List accountList = commonBL.getAccountDetail(account);
			Map map = (Map)accountList.get(0);
			List permissionList = commonBL.getAccountPermissions(account);
			HttpSession session = request.getSession();
			session.setAttribute("userDetail", map);
			session.setAttribute("userPermission", permissionList);
			return "/dashview/pages/index.html";
		}
		return "/dashview/pages/login.html";
	}

}
