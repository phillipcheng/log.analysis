package dv.ws;

import java.io.IOException;
import java.util.List;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import bdap.util.FileType;
import bdap.util.JsonUtil;
import dv.db.dao.FlowInstanceRepository;
import dv.db.dao.FlowRepository;
import dv.db.dao.ProjectRepository;
import dv.db.entity.FlowEntity;
import dv.db.entity.FlowInstanceEntity;
import dv.db.entity.ProjectEntity;
import etl.flow.Data;
import etl.flow.Flow;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;


@ServerEndpoint(value = "/ws/{userName}/flow/instances/{instanceId}/data/{dataName}", configurator = SpringEndpointConfigurator.class)
public class FlowInstanceDataHandler extends WebSocketServerEndpoint {
	public static final Logger logger = LogManager.getLogger(FlowDataHandler.class);
	private Session session;
	private Data data;
	private FlowInfo flowInfo;

	@Autowired
	private ProjectRepository projectRepository;
	@Autowired
	private FlowRepository flowRepository;
	@Autowired
	private FlowInstanceRepository flowInstanceRepository;
	@Autowired
	private FlowMgr flowMgr;
	@Autowired
	private FlowDeployer flowDeployer;
	
    @OnOpen
    public void onOpen(Session session) throws IOException {
    	this.session = session;

    	String userName;
    	String instanceId;
    	String dataName;
    	if (session.getPathParameters() != null) {
    		userName = session.getPathParameters().get("userName");
    		instanceId = session.getPathParameters().get("instanceId");
    		dataName = session.getPathParameters().get("dataName");
    	} else {
    		userName = null;
    		instanceId = null;
    		dataName = null;
    	}
		
    	validateUser(userName);

		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null)
			flowInfo = this.flowMgr.getFlowInfo(pe.getProjectName(), flowDeployer.getOozieServerConf(), instanceId);
		if (flowInfo != null) {
			if (flowEntity != null && flowEntity.getJsonContent() != null && flowEntity.getJsonContent().length() > 0) {
				Flow flow = JsonUtil.fromJsonString(flowEntity.getJsonContent(), Flow.class);
				List<Data> datalist = flow.getData();
				if (datalist != null) {
					for (Data d: datalist) {
						if (d != null && d.getName() != null && d.isInstance() && d.getName().equals(dataName)) {
							data = d;
						}
					}
				}
			} else {
				logger.debug("Flow {} not found", flowInfo.getName());
			}
		} else {
			logger.debug("Flow instance {} not found", instanceId);
		}
	}

    @OnMessage
    public void onMessage(Session session, String message) throws IOException {
		logger.info(message);
		TextFileOption option = JsonUtil.fromJsonString(message, TextFileOption.class);
		if (data != null) {
			if (option.getFilePath() != null && option.getFilePath().length() > 0) {
				InMemFile textFile = flowMgr.getDFSFile(flowDeployer.getEngineConfig(), option.getFilePath(),
						option.getStartLine(), option.getEndLine());
				if (textFile != null)
					session.getBasicRemote().sendText(JsonUtil.toJsonString(textFile));
				else
					session.getBasicRemote().sendText("");
			
			} else {
				InMemFile file = flowMgr.getDFSFile(flowDeployer.getEngineConfig(), data, flowInfo);;
				if (FileType.directoryList.equals(file.getFileType()))
					session.getBasicRemote().sendText(JsonUtil.toJsonString(file));
				else if (FileType.binaryData.equals(file.getFileType()))
					session.getBasicRemote().sendText(JsonUtil.toJsonString(file));
				else {
					file = flowMgr.getDFSFile(flowDeployer.getEngineConfig(), data.getLocation(),
							option.getStartLine(), option.getEndLine());
					session.getBasicRemote().sendText(JsonUtil.toJsonString(file));
				}
			}
		} else {
			session.getBasicRemote().sendText("");
		}
	}
    
    @OnClose
    public void onClose() {
    	logger.debug("Session {} closed", session.getId());
    }

}
