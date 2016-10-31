package dv.tableau.bl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import bdap.util.PropertiesUtil;
import bdap.util.XmlUtil;
import dv.db.dao.AccountRepository;
import dv.db.dao.AccountRepositoryImpl;
import dv.db.dao.CommonDaoImpl;
import dv.tableau.rest.ProjectListType;
import dv.tableau.rest.ProjectType;
import dv.tableau.rest.SiteType;
import dv.tableau.rest.TableauCredentialsType;
import dv.tableau.rest.TsResponse;
import dv.tableau.rest.WorkbookListType;
import dv.tableau.rest.WorkbookType;
import dv.util.RequestUtil;

@Service("TableauBL")
public class TableauBLImpl implements TableauBL {
	public static final Logger logger = LogManager.getLogger(TableauBLImpl.class);
	
	private static final String config = "config/config.properties";
	public static final String cfgkey_proxy_host="proxy.host";
	public static final String cfgkey_proxy_port="proxy.port";
	public static final String cfgkey_tableau_server_ip="tableau.server.ip";
	public static final String cfgkey_tableau_server_port="tableau.server.port";
	
	private String tableauServerIp;
	private int tableauServerPort;
	private String proxyHost;
	private int proxyPort;
	
	@Autowired
	CommonDaoImpl commonDao;

	public TableauBLImpl(){
		try{
			PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(config);
			tableauServerIp = pc.getString(cfgkey_tableau_server_ip, null);
			tableauServerPort = pc.getInt(cfgkey_tableau_server_port, 80);
			proxyHost = pc.getString(cfgkey_proxy_host, null);
			proxyPort = pc.getInt(cfgkey_proxy_port);
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	@Override
	public TsResponse signin(String username, String password, String siteName) {
		String url = String.format("http://%s:%d/api/2.0/auth/signin", tableauServerIp, tableauServerPort);
		StringBuffer buffer = new StringBuffer();
		buffer.append("<?xml version='1.0' encoding='UTF-8' ?>");
		buffer.append("<tsRequest xmlns='http://tableausoftware.com/api'>");
		buffer.append("    <credentials name='%s' password='%s'>");
		buffer.append("    	  <site contentUrl='%s' />");
		buffer.append("    </credentials>");
		buffer.append("</tsRequest>");
		String xml = buffer.toString();
		xml = String.format(xml, username, password, siteName);
		logger.info(xml);
		String response = RequestUtil.post(url, proxyHost, proxyPort, null, xml);
		logger.info(response);
		TsResponse returnTr = XmlUtil.xmlToBean(response, TsResponse.class);
		return returnTr;
	}

	public List<ProjectType> getProjects(String username, String password, String siteName) {
		TsResponse tr = signin(username, password, siteName);
		String url = String.format("http://%s:%d/api/2.0/sites/%s/projects", tableauServerIp, tableauServerPort, tr.getCredentials().getSite().getId());
		Map<String, String> headMap = new HashMap<String, String>();
		headMap.put("X-Tableau-Auth", tr.getCredentials().getToken());
		String response = RequestUtil.get(url, proxyHost, proxyPort, headMap);
		logger.info(response);
		TsResponse returnTr = XmlUtil.xmlToBean(response, TsResponse.class);
		ProjectListType type = returnTr.getProjects();
		List<ProjectType> list = type.getProject();
		return list;
	}

	@Override
	public List<WorkbookType> getWorkbooksBySite(String username, String password, String siteName) {
		TsResponse tr = signin(username, password, siteName);
		String url = String.format("http://%s:%d/api/2.0/sites/%s/users/%s/workbooks", tableauServerIp, tableauServerPort, 
				tr.getCredentials().getSite().getId(), tr.getCredentials().getUser().getId());
		Map<String, String> headMap = new HashMap<String, String>();
		headMap.put("X-Tableau-Auth", tr.getCredentials().getToken());
		String response = RequestUtil.get(url, proxyHost, proxyPort, headMap);
		logger.info(response);
		TsResponse returnTr = XmlUtil.xmlToBean(response, TsResponse.class);
		WorkbookListType type = returnTr.getWorkbooks();
		List<WorkbookType> list = type.getWorkbook();
		return list;
	}
	
	private String getProjectidByName(List<ProjectType> list, String projectName) {
		String projectid = "";
		if(list != null && list.size() > 0) {
			for(ProjectType project : list) {
				if(projectName.equals(project.getName())) {
					projectid = project.getId();
				}
			}
		}
		return projectid;
	}
	
	
	
	
	
}
