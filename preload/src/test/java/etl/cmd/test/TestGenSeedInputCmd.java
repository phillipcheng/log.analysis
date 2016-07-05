package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.GenSeedInputCmd;
import etl.util.Util;

public class TestGenSeedInputCmd extends TestETLCmd {

	public static final Logger logger = Logger.getLogger(TestGenSeedInputCmd.class);

	@Test
	public void test1()  throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				BufferedReader br = null;
				try {
					//
					String inputFolder = "/test/genseedinputcmd/input/";
					String outputFolder = "/test/genseedinputcmd/output/";
					String dfsCfgFolder = "/test/genseedinputcmd/cfg/";

					String staticCfgName = "genseedinput1.properties";
					String wfid = "wfid1";

					List<String> inputFiles = new ArrayList<String> (Arrays.asList( "file1", "file2", "file3" ));

					String localFile = "backup_test1_data";

					// generate all the data files
					getFs().delete(new Path(inputFolder), true);
					getFs().delete(new Path(outputFolder), true);
					getFs().delete(new Path(dfsCfgFolder), true);
					getFs().mkdirs(new Path(inputFolder));
					getFs().mkdirs(new Path(outputFolder));
					getFs().mkdirs(new Path(dfsCfgFolder));
					for (String inputFile : inputFiles) {
						getFs().copyFromLocalFile(new Path(getLocalFolder() + localFile),
								new Path(inputFolder + inputFile));
					}
					// add the local conf file to dfs
					getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName),
							new Path(dfsCfgFolder + staticCfgName));

					// run cmd
					GenSeedInputCmd cmd = new GenSeedInputCmd(wfid, dfsCfgFolder + staticCfgName, null, null,
							getDefaultFS());
					List<String> seedNames = cmd.process(0, null, null);
					// check results
					// outputFolder should have the seed file
					assertTrue(seedNames != null);
					assertTrue(seedNames.size() == 1);
					List<String> flist = Util.listDfsFile(getFs(), outputFolder);
					logger.info(flist);
					logger.info("Output:" + seedNames);
					assertTrue(flist.size() == 1);
					assertTrue(seedNames.get(0).endsWith(flist.get(0)));
					// reading file content and verifying
					logger.info(inputFiles);
					br = new BufferedReader(
							new InputStreamReader(getFs().open(new Path(seedNames.get(0)))));

					String line;
					while ((line = br.readLine()) != null) {
						logger.info(line);
						assertTrue(inputFiles.contains(line.trim()));
					}

				} catch (Exception e) {
					logger.error("Exception occured :", e);
				} finally {
					if(br!=null)
					br.close();
				}
				return null;
			}
		});
	}
	
	@Test
	public void remoteTest1() throws Exception{
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				test1();
				return null;
			}
		});
		
	}
}