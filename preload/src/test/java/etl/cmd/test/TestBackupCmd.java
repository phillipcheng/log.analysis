package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import etl.cmd.BackupCmd;
import etl.util.Util;

import org.apache.log4j.Logger;

public class TestBackupCmd extends TestETLCmd{

	public static final Logger logger = Logger.getLogger(TestBackupCmd.class);

	private void test1Fun() throws Exception{
		try {
			//
			String dynFolder = "/test/BackupCmd/data/dynFolder1/";
			String allFolder = "/test/BackupCmd/data/allFolder1/";
			String wfidFolder = "/test/BackupCmd/data/wfidFolder1/";

			String[] dynCfgFileNames = new String[]{"dynCfgFile1", "dynCfgFile2"};
			String dfsCfgFolder = "/test/BackupCmd/cfg/";
			String dynCfgName = "dynCfg1";
			String staticCfgName = "backup_test1_staticCfg.properties";
			String wfid="wfid1";

			String[] dynFiles = new String[]{"dynCfgFile1", "dynCfgFile2", "dynCfgFile3"};
			String[] allFiles = new String[]{"all1", "all2"};
			String[] wfidFiles = new String[]{wfid+"a", wfid+"b", "a"};

			String localFile = "backup_test1_data";
			//values should be in the configuration file
			String dynKey = "dynFiles";
			String historyFolder = "/test/datahistory/";
			//generate all the data files
			getFs().delete(new Path(dynFolder), true);
			getFs().delete(new Path(allFolder), true);
			getFs().delete(new Path(wfidFolder), true);
			for (String dynFile: dynFiles){
				getFs().copyFromLocalFile(new Path(getLocalFolder() + localFile), new Path(dynFolder + dynFile));
			}
			for (String allFile: allFiles){
				getFs().copyFromLocalFile(new Path(getLocalFolder() + localFile), new Path(allFolder + allFile));
			}
			for (String wfidFile: wfidFiles){
				getFs().copyFromLocalFile(new Path(getLocalFolder() + localFile), new Path(wfidFolder + wfidFile));
			}

			//generate local dyn conf file
			Map<String, List<String>> dynCfgValues = new HashMap<String, List<String>>();
			dynCfgValues.put(dynKey, Arrays.asList(dynCfgFileNames));
			Util.toLocalJsonFile(getLocalFolder() + dynCfgName, dynCfgValues);

			//add the local conf file to dfs
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsCfgFolder + staticCfgName));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + dynCfgName), new Path(dfsCfgFolder + dynCfgName));

			//run cmd
			BackupCmd cmd = new BackupCmd(wfid, dfsCfgFolder + staticCfgName, dfsCfgFolder + dynCfgName, getDefaultFS());
			cmd.sgProcess();

			//check results
			//dynFolder should only contains 1 file
			List<String> flist;
			flist = Util.listDfsFile(getFs(), dynFolder);
			assertTrue(flist.size()==1);
			assertTrue(flist.contains("dynCfgFile3"));
			//allFolder should be empty
			flist = Util.listDfsFile(getFs(), allFolder);
			assertTrue(flist.size()==0);
			//wfidFolder should have only 1 file
			flist = Util.listDfsFile(getFs(), wfidFolder);
			assertTrue(flist.size()==1);
			assertTrue(flist.contains("a"));
			//check the zip file
			flist = Util.listDfsFile(getFs(), historyFolder);
			assertTrue(flist.contains(wfid+".zip"));
		} catch (Exception e) {
			logger.error("Exception occured ", e);
		}
	}

	@Test
	public void test1() throws Exception{
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