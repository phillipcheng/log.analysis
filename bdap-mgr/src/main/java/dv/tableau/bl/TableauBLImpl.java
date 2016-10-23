package dv.tableau.bl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.PropertiesUtil;
import bdap.util.XmlUtil;
import dv.tableau.rest.TsResponse;
import dv.util.RequestUtil;

public class TableauBLImpl implements TableauBL {
	public static final Logger logger = LogManager.getLogger(TableauBLImpl.class);
	
	public static final String cfgkey_proxy_host="proxy.host";
	public static final String cfgkey_proxy_port="proxy.port";
	public static final String cfgkey_tableau_server_ip="tableau.server.ip";
	public static final String cfgkey_tableau_server_port="tableau.server.port";
	
	private String tableauServerIp;
	private int tableauServerPort;
	private String proxyHost;
	private int proxyPort;
	
	public TableauBLImpl(String config){
		PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(config);
		tableauServerIp = pc.getString(cfgkey_tableau_server_ip, null);
		tableauServerPort = pc.getInt(cfgkey_tableau_server_port, 80);
		proxyHost = pc.getString(cfgkey_proxy_host, null);
		proxyPort = pc.getInt(cfgkey_proxy_port);
	}
	
	@Override
	public TsResponse signin(String username, String password) {
		String url = String.format("http://%s:%d/api/2.0/auth/signin", tableauServerIp, tableauServerPort);
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
		TsResponse returnTr = XmlUtil.xmlToBean(response, TsResponse.class);
		return returnTr;
	}

	@Override
	public TsResponse getProjects(String username, String password) {
		TsResponse tr = signin(username, password);
		String url = String.format("http://%s:%d/api/2.0/sites/%s/projects", tableauServerIp, tableauServerPort, tr.getCredentials().getSite().getId());
		Map<String, String> headMap = new HashMap<String, String>();
		headMap.put("X-Tableau-Auth", tr.getCredentials().getToken());
		String response = RequestUtil.get(url, proxyHost, proxyPort, headMap);
		logger.info(response);
		TsResponse returnTr = XmlUtil.xmlToBean(response, TsResponse.class);
		return returnTr;
	}
	
}
