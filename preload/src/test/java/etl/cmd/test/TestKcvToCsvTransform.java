package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestKcvToCsvTransform extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestKcvToCsvTransform.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static final String cmdClassName = "etl.cmd.transform.KcvToCsvCmd";

	private void test1Fun() throws Exception{
		BufferedReader br = null;
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/kcvtransform/";
			String remoteCsvOutputFolder = "/etltest/kcvtransformout/";
			// setup testing env
			String kcvtransProp = "kcv2csv.properties";
			String[] kcvFiles = new String[]{"PJ24002A_BBG2.fix"};
			// run job
			List<String> retlist = super.mapTest(remoteCfgFolder, remoteCsvFolder, remoteCsvOutputFolder, kcvtransProp, kcvFiles, 
					cmdClassName, true);
			String regexPattern = "[\\s]+([A-Za-z0-9\\,\\. ]+)[\\s]+([A-Z][A-Z ]+)";
			String line = null;
			
			logger.info(retlist);
			assertTrue(retlist != null);
			assertTrue(retlist.size() > 0);
			br = new BufferedReader(new InputStreamReader(getFs().open(new Path(remoteCsvFolder + kcvFiles[0]))));
			if ((line = br.readLine()) != null) {
				Pattern pattern = Pattern.compile(regexPattern);
				Matcher matcher = pattern.matcher(line);
				if (matcher.find()) {
					assertTrue(retlist.get(0).trim().startsWith(matcher.group(1)));
				}
			}

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			if (br != null)
				br.close();
		}
	}
	
	@Test
	public void test1() throws Exception {
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
