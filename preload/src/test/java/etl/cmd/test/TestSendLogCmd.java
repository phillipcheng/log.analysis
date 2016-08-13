package etl.cmd.test;

import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.SendLogCmd;

public class TestSendLogCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestSendLogCmd.class);

	private void test1Fun() throws Exception {
		String dfsFolder = "/test/sendlog/";
		String staticCfgName = "sendlog.properties";
		String wfid = "wf1";
		
		getFs().delete(new Path(dfsFolder), true);
		getFs().mkdirs(new Path(dfsFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsFolder + staticCfgName));
		
		// run cmd
		String[] infoArray = new String[]{"cmd1", "1000"};
		SendLogCmd cmd = new SendLogCmd(wfid, dfsFolder + staticCfgName, getDefaultFS(), infoArray);
		cmd.sgProcess();
	}
	
	@Test
	public void test1() throws Exception{
		if (!super.isTestKafka()) return;
		if (getDefaultFS().contains("127.0.0.1")){
			test1Fun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					test1Fun();
					return null;
				}
			});
		}
	}
}
