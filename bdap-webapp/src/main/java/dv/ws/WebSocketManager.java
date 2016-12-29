package dv.ws;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ServerEndpoint(value = "/ws/{userName}/flow/instances/{instanceId}", configurator = SpringEndpointConfigurator.class)
public class WebSocketManager extends WebSocketServerEndpoint {
	public static final Logger logger = LogManager.getLogger(WebSocketManager.class);
	private static final String INSTANCE_ID = "FlowInstanceId";
	//concurrent is safe thread, to save each connection.
    private static CopyOnWriteArraySet<WebSocketManager> webSocketSet = new CopyOnWriteArraySet<WebSocketManager>();
    //session, send messages by it.
    private Session session;

    /**
     * when connection is successful, it will be triggered.
     * @param session
     */
    @OnOpen
    public void onOpen(Session session) {
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
    	
    	session.getUserProperties().put(INSTANCE_ID, instanceId);
        
      //add to set
        webSocketSet.add(this);
    }

    /**
     * when connection is closed, it will be triggered.
     */
    @OnClose
    public void onClose() {
    	//remove from set
        webSocketSet.remove(this);
    }

    /**
     * receive messages from client.
     * when there is message coming, it will be triggered.
     * @param message
     * @param session
     */
    @OnMessage
    public void onMessage(Session session, String message) throws IOException {
    	logger.debug(message);
    }

    /**
     * when error happen, it will be triggered.
     */
    
    @OnError
    public void onError(Session session, Throwable error) {
        logger.error(error.getMessage(), error);
    }

    /**
     * send JSON format string, UI js will parse them.
     * @param message
     * @throws IOException
     */
    public void sendMessage(String message) throws IOException {
        this.session.getBasicRemote().sendText(message);
    }

	private boolean subscribed(String instanceId) {
		return this.session != null && this.session.getUserProperties() != null && instanceId != null &&
				instanceId.equals(this.session.getUserProperties().get(INSTANCE_ID));
	}
    
    /**
     * send mass messages 
     * */
    public static void sendMassMessage(String instanceId, String message) throws IOException {
        for (WebSocketManager item : webSocketSet) {
        	if (item.subscribed(instanceId)) {
	            try {
	                item.sendMessage(message);
	            } catch (IOException e) {
	            	logger.error(e.getMessage(), e);
	                continue;
	            }
        	}
        }
    }
    
}
