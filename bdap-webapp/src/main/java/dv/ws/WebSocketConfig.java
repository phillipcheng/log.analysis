package dv.ws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

@Configuration
public class WebSocketConfig {  
	@Bean
    public SpringEndpointConfigurator springEndpointConfigurator() {
        return new SpringEndpointConfigurator(); // This is just to get context
    }
	
    @Bean  
    public ServerEndpointExporter serverEndpointExporter (){  
        return new ServerEndpointExporter();  
    }
    
	@Bean
	public WebSocketManager webSocketManager() {
		return new WebSocketManager();
	}
	
    @Bean
    public FlowInstanceLogHandler flowInstanceLogHandler() {
        return new FlowInstanceLogHandler();
    }
    
    @Bean
    public FlowNodeInstanceLogHandler flowNodeInstanceLogHandler() {
        return new FlowNodeInstanceLogHandler();
    }
    
    @Bean
    public FlowDataHandler flowDataHandler() {
        return new FlowDataHandler();
    }
    
    @Bean
    public FlowInstanceDataHandler flowInstanceDataHandler() {
        return new FlowInstanceDataHandler();
    }
}
