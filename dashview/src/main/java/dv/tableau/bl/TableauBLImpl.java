package dv.tableau.bl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dv.tableau.rest.TsResponse;
import dv.util.ConfigManager;
import dv.util.RequestUtil;
import dv.util.XmlUtil;

public class TableauBLImpl implements TableanBL {
	public static final Logger logger = LogManager.getLogger(TableauBLImpl.class);
	
	private static String tableauip;
	private static String proxyHost;
	private static String proxyPort;
	static {
		if (StringUtils.isEmpty(tableauip)) {
			Map configMap = ConfigManager.getProperties();
			tableauip = (String)configMap.get("tableauip");
			proxyHost = (String)configMap.get("proxyHost");
			proxyPort = (String)configMap.get("proxyPort");
		}
	}
	@Override
	public TsResponse signin(String username, String password) {
		String url = "http://%s/api/2.0/auth/signin";
		url = String.format(url, tableauip);
		StringBuffer buffer = new StringBuffer();
		buffer.append("<?xml version='1.0' encoding='UTF-8' ?>");
		buffer.append("<tsRequest xmlns='http://tableausoftware.com/api'>");
		buffer.append("    <credentials name='%s' password='%s'>");
		buffer.append("    	  <site contentUrl='' />");
		buffer.append("    </credentials>");
		buffer.append("</tsRequest>");
		String xml = buffer.toString();
		xml = String.format(xml, username, password);
		logger.info(xml);
		String response = RequestUtil.post(url, proxyHost, proxyPort, null, xml);
		logger.info(response);
		TsResponse returnTr = (TsResponse)XmlUtil.XMLStringToBean(response, TsResponse.class);
		return returnTr;
	}

	@Override
	public TsResponse getProjects(String username, String password) {
		TsResponse tr = signin(username, password);
		String url = "http://%s/api/2.0/sites/%s/projects";
		url = String.format(url, tableauip, tr.getCredentials().getSite().getId());
		Map<String, String> headMap = new HashMap<String, String>();
		headMap.put("X-Tableau-Auth", tr.getCredentials().getToken());
		String response = RequestUtil.get(url, proxyHost, proxyPort, headMap);
		logger.info(response);
		TsResponse returnTr = (TsResponse)XmlUtil.XMLStringToBean(response, TsResponse.class);
		return returnTr;
	}
	
}
