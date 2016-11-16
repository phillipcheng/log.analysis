package etl.engine.test;

import java.io.ByteArrayOutputStream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.junit.Test;

public class TestShell {
	
	@Test
	public void getShellOutput() throws Exception {
	    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
	    PumpStreamHandler psh = new PumpStreamHandler(stdout);
	    CommandLine cl = CommandLine.parse("C:\\mydoc\\myprojects\\bdap\\bdap-engine\\src\\test\\resources\\shell\\copyfile.bat C:\\TestShell\\backupfolder C:\\TestShell\\original");
	    DefaultExecutor exec = new DefaultExecutor();
	    exec.setStreamHandler(psh);
	    exec.execute(cl);
	    String ret=stdout.toString();
	    System.out.println(stdout.toString());
	}

}
