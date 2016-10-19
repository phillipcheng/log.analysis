package dv.tableau.bl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dom4j.DocumentException;
import org.springframework.beans.factory.annotation.Autowired;

import dv.entity.AccountRepository;
import dv.entity.FlowRepository;
import dv.util.ConfigManager;
import dv.util.RequestUtil;
import dv.util.XmlUtil;

public class TableauBLImpl implements TableanBL {
	
	private static String tableauip="";
	private static String username="";
	private static String password="";
	static {
		if (StringUtils.isEmpty(tableauip)) {
			Map configMap = ConfigManager.getProperties();
			tableauip = (String)configMap.get("tableauip");
			username = (String)configMap.get("username");
			password = (String)configMap.get("password");
		}
	}
	@Override
	public Map signin() {
		String url = "http://###tableauip/api/2.0/auth/signin";
		url = url.replace("###tableauip", tableauip);
		StringBuffer buffer = new StringBuffer();
		buffer.append("<tsRequest>");
		buffer.append("    <credentials name='###username' password='###password'>");
		buffer.append("    	  <site contentUrl='' />");
		buffer.append("    </credentials>");
		buffer.append("</tsRequest>");
		String xml = buffer.toString();
		xml = xml.replace("###username", username).replace("###password", password);
		String response = RequestUtil.post(url, null, xml);
		Map<String, Object> maprest = null;
		try {
			maprest = XmlUtil.xml2mapWithAttr(response, true);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		return maprest;
	}

	@Override
	public Map getProjects() {
		Map<String, Object> maprest = null;
		try {
			Map<String, Object> map = signin();
			Map map1 = (Map) map.get("tsResponse");
			Map map2 = (Map) map1.get("credentials");
			Map map3 = (Map) map2.get("site");
			String token = (String) map2.get("@token");
			String siteid = (String) map3.get("@id");
			String url = "http://###tableauip/api/2.0/sites/###siteid/projects";
			url = url.replace("###tableauip", tableauip);
			url = url.replace("###siteid", siteid);
			Map<String, String> headMap = new HashMap<String, String>();
			headMap.put("X-Tableau-Auth", token);
			String response = RequestUtil.get(url, headMap);
			maprest = new XmlUtil().xml2mapWithAttr(response, true);
			System.out.println(maprest);
		} catch (DocumentException e) {
			e.printStackTrace();
		}

		return maprest;
	}
}
