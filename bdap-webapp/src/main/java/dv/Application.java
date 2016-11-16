package dv;

import java.util.HashSet;
import java.util.Set;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;




@SpringBootApplication
@Configuration
//@EnableWebMvc  //add it, static resource cannot request,show 404 error
@ImportResource({
    "classpath:/config/jpa.xml",
})
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}




