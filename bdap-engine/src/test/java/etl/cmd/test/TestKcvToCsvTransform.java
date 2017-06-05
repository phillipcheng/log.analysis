package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.input.KCVInputFormat;

public class TestKcvToCsvTransform extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestKcvToCsvTransform.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static final String cmdClassName = "etl.cmd.KcvToCsvCmd";

	@Test
	public void test1() throws Exception{
		BufferedReader br = null;
		try {
			String remoteCsvFolder = "/etltest/kcvtransform/";
			String remoteCsvOutputFolder = "/etltest/kcvtransformout/";
			// setup testing env
			String kcvtransProp = "kcv2csv.properties";
			String[] kcvFiles = new String[]{"PJ24002A_BBG2.fix"};
			// run job
			getConf().set("header.line", "TIME:\\s*\\S+\\s*UNCERTAINTY:\\s*\\S+\\s*SOURCE:");
			getConf().set("footer.line", "");
			List<String> retlist = super.mapTest(remoteCsvFolder, remoteCsvOutputFolder, kcvtransProp, kcvFiles, 
					cmdClassName, KCVInputFormat.class);
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

	@Override
	public String getResourceSubFolder() {
		return "kcvToCsv/";
	}
}
