package etl.flow.test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.Test;

import etl.flow.deploy.SSHDeployMethod;

public class TestSSHDeployMethod {

	@Test
	public void test() {
		SSHDeployMethod m = new SSHDeployMethod("192.85.247.104", 22, "dbadmin", "password", "/data/hadoop-2.7.3", "/tmp/");
		
		m.copyFromLocalFile("pom.xml", "/user/dbadmin/pom.xml");
		
		List<String> result = m.readFile("/user/dbadmin/pom.xml");
		
		System.out.println(result);
		
		m.delete("/user/dbadmin/pom.xml", true);
		
		m.createFile("/user/dbadmin/test.txt", "Hello World!".getBytes(StandardCharsets.UTF_8));
		
		result = m.readFile("/user/dbadmin/test.txt");
		
		System.out.println(result);
		
		m.delete("/user/dbadmin/test.txt", true);
		
		m.listFiles("/user/dbadmin");
	}

}
