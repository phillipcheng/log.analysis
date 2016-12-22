package etl.flow.test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import etl.flow.deploy.FTPDeployMethod;

public class TestFTPDeployMethod {

	public static void main(String[] args) {
		FTPDeployMethod m = new FTPDeployMethod("127.0.0.1", 21, "player", "123456");
		
		m.copyFromLocalFile("pom.xml", "/user/player/pom.xml");
		
		List<String> result = m.readFile("/user/player/pom.xml");
		
		System.out.println(result);
		
		m.delete("/user/player/pom.xml", true);
		
		m.createFile("/user/player/test.txt", "Hello World!".getBytes(StandardCharsets.UTF_8));
		
		result = m.readFile("/user/player/test.txt");
		
		System.out.println(result);
		
		m.delete("/user/player/test.txt", true);
		
		m.listFiles("/user/player");
	}

}
