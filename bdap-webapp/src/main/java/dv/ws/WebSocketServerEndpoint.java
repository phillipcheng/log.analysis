package dv.ws;

import javax.websocket.OnError;
import javax.websocket.Session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import dv.UserNotFoundException;
import dv.db.dao.AccountRepository;
import dv.db.entity.AccountEntity;

public abstract class WebSocketServerEndpoint {
	public static final Logger logger = LogManager.getLogger(WebSocketServerEndpoint.class);
	
	@Autowired
	private AccountRepository accountRepository;
	
    @OnError
    public void onError(Session session, Throwable error) {
        logger.error(error.getMessage(), error);
    }
    
	protected void validateUser(String userName) {
		AccountEntity ae = this.accountRepository.findOne(userName);
		if (ae==null){
			throw new UserNotFoundException(userName);
		}
	}
}
