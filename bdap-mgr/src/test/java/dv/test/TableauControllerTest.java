package dv.test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import dv.Application;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;
/**
 * 
 * @author ximing
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = Application.class)
@WebAppConfiguration
public class TableauControllerTest {

	private MediaType contentType = new MediaType(MediaType.APPLICATION_JSON.getType(),
            MediaType.APPLICATION_JSON.getSubtype(),
            Charset.forName("utf8"));

	private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext webApplicationContext;
    
    @Before
    public void setup() throws Exception {
        this.mockMvc = webAppContextSetup(webApplicationContext).build();
    }
    
    @Test
    public void signin() throws Exception {
    	MockHttpSession session = getSession();
        mockMvc.perform(get("/tableau/rest/signin").session(session));
    }
    
    @Test
    public void allProjects() throws Exception {
    	MockHttpSession session = getSession();
        mockMvc.perform(get("/tableau/rest/allProjects").session(session));
    }
    
    @Test
    public void allWorkbooks() throws Exception {
    	MockHttpSession session = getSession();
        mockMvc.perform(get("/tableau/rest/allWorkbooks").session(session));
    }
    
    private MockHttpSession getSession(){
    	MockHttpSession session = new MockHttpSession();
    	Map map = new HashMap();
    	map.put("name", "admin");
    	map.put("password", "admin123");
    	session.setAttribute("userDetail", map);
    	return session;
    }
   
   

}
