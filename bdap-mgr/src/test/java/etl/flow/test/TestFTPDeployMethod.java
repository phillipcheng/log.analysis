package etl.flow.test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.Test;

import etl.flow.deploy.FTPDeployMethod;

public class TestFTPDeployMethod {

	@Test
	public void test() {
		FTPDeployMethod m = new FTPDeployMethod("192.85.247.104", 21, "dbadmin", "123456");
		
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
