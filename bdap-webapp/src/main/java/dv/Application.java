package dv;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;


@SpringBootApplication
@Configuration
//@EnableWebMvc  //add it, static resource cannot request,show 404 error
@ImportResource({
    "classpath:/config/jpa.xml",
    "classpath:/config/mgr.xml"
})
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}




