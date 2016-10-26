package dv.mgr;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
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

//@RestController
@Controller
@RequestMapping("/login")
public class LoginController {
	@Autowired
	CommonBLImpl commonBL;
	
	@RequestMapping(value = "/signin",method = RequestMethod.POST)
    public ModelAndView signin (@ModelAttribute AccountEntity account, Model model, HttpServletRequest request){
		boolean enableLogin = commonBL.validateLogin(account);
		ModelAndView modelView = new ModelAndView();
		if(enableLogin){
			List list = commonBL.getAccountDetail(account);
			HttpSession session = request.getSession();
			session.setAttribute("userDetail", list);
			modelView.setViewName("redirect:/pages/index.html");
			return modelView;
		}
		modelView.addObject("error", new String("login fail!"));
		modelView.setViewName("redirect:/pages/login.html");
		return modelView;
	}

}
