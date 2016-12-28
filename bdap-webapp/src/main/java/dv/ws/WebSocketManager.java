package dv.ws;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.JsonUtil;


@ServerEndpoint(value = "/websocket")
public class WebSocketManager {
	public static final Logger logger = LogManager.getLogger(WebSocketManager.class);
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
      //add to set
        webSocketSet.add(this);
        try {
            sendMessage("test");
        } catch (IOException e) {
        	logger.error(e.getMessage(), e);
        }
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
    	logger.info(message);
    	try {
			this.sendMessage(message);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
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
    
    /**
     * only test send message.
     */
    public void testSendMessage(){
    	//below is sending a JSON string case.
        Map map = new HashMap();
        map.put("info", "this is info log");
        map.put("debug", "this is debug log");
        map.put("warning", "this is warning log");
        map.put("error", "this is error log");
        map.put("key5", "value1");
        String json = JsonUtil.toJsonString(map);
        try {
			sendMessage(json);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
    }

    /**
     * send mass messages 
     * */
    public static void sendMassMessage(String message) throws IOException {
        for (WebSocketManager item : webSocketSet) {
            try {
                item.sendMessage(message);
            } catch (IOException e) {
                continue;
            }
        }
    }
    
    
}