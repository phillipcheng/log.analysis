package dv.ws;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;

import bdap.util.FileType;
import bdap.util.JsonUtil;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;


@ServerEndpoint(value = "/ws/{userName}/flow/instances/{instanceId}/nodes/{nodeName}/log", configurator = SpringEndpointConfigurator.class)
public class FlowNodeInstanceLogHandler extends WebSocketServerEndpoint {
	public static final Logger logger = LogManager.getLogger(FlowDataHandler.class);
	private static final String LOG_FILE_PATH = "LogFilePath";
	private Session session;
	
	@Autowired
	private FlowMgr flowMgr;
	@Autowired
	private FlowDeployer flowDeployer;
	
    @OnOpen
    public void onOpen(Session session) throws IOException {
    	logger.debug("Session {} opened", session.getId());
    	
    	this.session = session;

    	String userName;
    	String instanceId;
    	String nodeName;
    	if (session.getPathParameters() != null) {
    		userName = session.getPathParameters().get("userName");
    		instanceId = session.getPathParameters().get("instanceId");
    		nodeName = session.getPathParameters().get("nodeName");
    	} else {
    		userName = null;
    		instanceId = null;
    		nodeName = null;
    	}
		
    	validateUser(userName);
    	
    	Resource[] resources = flowMgr.getNodeLogResources("project1", flowDeployer.getOozieServerConf(), instanceId, nodeName);
    	String projectDir;
    	String logFilePath;
    	
    	if (resources != null) {
    		InputStream in;
    		String fileName;
    		List<String> logFilePaths = new ArrayList<String>();
    		for (Resource resource: resources) {
	    		in = null;
		    	try {
			    	in = resource.getInputStream();
			    	projectDir = flowDeployer.getProjectHdfsDir("project1");
			    	if (!projectDir.endsWith(Path.SEPARATOR))
			    		projectDir += Path.SEPARATOR;
			    	fileName = resource.getFilename();
			    	if (fileName != null) {
			    		fileName = fileName.substring(fileName.indexOf("container_"));
			    		fileName = fileName.substring(0, fileName.indexOf("?start="));
			    	}
			    	logFilePath = projectDir + "logs" + Path.SEPARATOR + instanceId + Path.SEPARATOR + fileName;
			    	flowDeployer.deploy(logFilePath, in);
			    	logFilePaths.add(logFilePath);
		    	} finally {
		    		if (in != null)
		    			in.close();
		    	}
    		}
	    	session.getUserProperties().put(LOG_FILE_PATH, logFilePaths);
    	}
    }
    
    @OnMessage
    public void onMessage(Session session, String message) throws IOException {
		logger.info(message);
		TextFileOption option = JsonUtil.fromJsonString(message, TextFileOption.class);
		if (option != null && option.getFilePath() != null && option.getFilePath().length() > 0) {
			InMemFile textFile = flowMgr.getDFSFile(flowDeployer.getEngineConfig(), option.getFilePath(),
					option.getStartLine(), option.getEndLine());
			if (textFile != null)
				session.getBasicRemote().sendText(JsonUtil.toJsonString(textFile));
			else
				session.getBasicRemote().sendText("");
			
		} else if (session.getUserProperties().containsKey(LOG_FILE_PATH)) {
	    	String instanceId;
	    	if (session.getPathParameters() != null) {
	    		instanceId = session.getPathParameters().get("instanceId");
	    	} else {
	    		instanceId = null;
	    	}

	    	String projectDir = flowDeployer.getProjectHdfsDir("project1");
	    	if (!projectDir.endsWith(Path.SEPARATOR))
	    		projectDir += Path.SEPARATOR;
			
	    	String filePath = projectDir + "logs" + Path.SEPARATOR + instanceId + Path.SEPARATOR;
			List<String> logFilePaths = (List<String>) session.getUserProperties().get(LOG_FILE_PATH);
			InMemFile fileList = new InMemFile(FileType.directoryList, filePath, directoryList(logFilePaths));

			session.getBasicRemote().sendText(JsonUtil.toJsonString(fileList));
			
		} else {
			session.getBasicRemote().sendText("");
		}
	}
    
	private String directoryList(List<String> list) {
		StringBuilder buffer = new StringBuilder();
		if (list != null) {
			for (String f: list) {
				buffer.append(f);
				buffer.append("\n");
			}
		}
		return buffer.toString();
	}
    
    @OnClose
    public void onClose() {
    	logger.debug("Session {} closed", session.getId());
    	if (session.getUserProperties().containsKey(LOG_FILE_PATH)) {
    		List<String> logFilePaths = (List<String>) session.getUserProperties().get(LOG_FILE_PATH);
    		if (logFilePaths != null)
    			for (String filePath: logFilePaths)
    				flowDeployer.delete(filePath, false);
    	}
    }

}
