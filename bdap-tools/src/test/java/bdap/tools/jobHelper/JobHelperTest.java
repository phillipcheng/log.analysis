package bdap.tools.jobHelper;

import java.net.URL;

import org.junit.Test;

public class JobHelperTest {

	@Test
	public void test() throws Throwable {
		URL configFileUrl=ClassLoader.getSystemResource("testconfig.properties");
		
//		String[] args={"-help","-config",configFileUrl.toString(),"-scan","c:/tmp/scan_output.csv"};
		String[] args={"-help","-config",configFileUrl.toString(),"-rerun","c:/tmp/rerunjobs.csv", "c:/tmp/rerun_output.csv"};
		JobHelper.main(args);
	}
}
