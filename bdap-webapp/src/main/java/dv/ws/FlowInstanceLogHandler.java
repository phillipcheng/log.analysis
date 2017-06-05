package dv.ws;

import java.io.IOException;
import java.io.InputStream;

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

import bdap.util.JsonUtil;
import dv.db.dao.FlowInstanceRepository;
import dv.db.dao.FlowRepository;
import dv.db.dao.ProjectRepository;
import dv.db.entity.FlowEntity;
import dv.db.entity.FlowInstanceEntity;
import dv.db.entity.ProjectEntity;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;


@ServerEndpoint(value = "/ws/{userName}/flow/instances/{instanceId}/log", configurator = SpringEndpointConfigurator.class)
public class FlowInstanceLogHandler extends WebSocketServerEndpoint {
	public static final Logger logger = LogManager.getLogger(FlowDataHandler.class);
	private static final String LOG_FILE_PATH = "LogFilePath";
	private Session session;
	
	@Autowired
	private FlowMgr flowMgr;
	@Autowired
	private FlowDeployer flowDeployer;
	@Autowired
	private ProjectRepository projectRepository;
	@Autowired
	private FlowRepository flowRepository;
	@Autowired
	private FlowInstanceRepository flowInstanceRepository;
	
    @OnOpen
    public void onOpen(Session session) throws IOException {
    	logger.debug("Session {} opened", session.getId());
    	logger.info(session.getPathParameters());
    	
    	this.session = session;

    	String userName;
    	String instanceId;
    	if (session.getPathParameters() != null) {
    		userName = session.getPathParameters().get("userName");
    		instanceId = session.getPathParameters().get("instanceId");
    	} else {
    		userName = null;
    		instanceId = null;
    	}
		
    	validateUser(userName);

		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null) {
	    	Resource resource = flowMgr.getFlowLogResource(pe.getProjectName(), flowDeployer.getOozieServerConf(), instanceId);
	    	String projectDir;
	    	String logFilePath;
	    	
	    	if (resource != null) {
	    		InputStream in = null;
		    	try {
			    	in = resource.getInputStream();
			    	projectDir = flowDeployer.getProjectHdfsDir(pe.getProjectName());
			    	if (!projectDir.endsWith(Path.SEPARATOR))
			    		projectDir += Path.SEPARATOR;
			    	logFilePath = projectDir + "logs" + Path.SEPARATOR + instanceId + Path.SEPARATOR + "log.txt";
			    	session.getUserProperties().put(LOG_FILE_PATH, logFilePath);
			    	flowDeployer.deploy(logFilePath, in);
		    	} finally {
		    		if (in != null)
		    			in.close();
		    	}
	    	}
		}
    }

    @OnMessage
    public void onMessage(Session session, String message) throws IOException {
		logger.info(message);
		TextFileOption option = JsonUtil.fromJsonString(message, TextFileOption.class);
		if (session.getUserProperties().containsKey(LOG_FILE_PATH) && option != null) {
			String filePath = session.getUserProperties().get(LOG_FILE_PATH).toString();
			InMemFile textFile = flowMgr.getDFSFile(flowDeployer.getEngineConfig(), filePath,
					option.getStartLine(), option.getEndLine());
			session.getBasicRemote().sendText(JsonUtil.toJsonString(textFile));
		} else {
			session.getBasicRemote().sendText("");
		}
	}

    @OnClose
    public void onClose() {
    	logger.debug("Session {} closed", session.getId());
    	if (session.getUserProperties().containsKey(LOG_FILE_PATH)) {
    		String filePath = session.getUserProperties().get(LOG_FILE_PATH).toString();
        	flowDeployer.delete(filePath, false);
    	}
    }
}
