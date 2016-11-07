package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.List;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import bdap.util.HdfsUtil;
import bdap.util.Util;
import etl.cmd.BackupCmd;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestBackupCmd extends TestETLCmd{

	public static final Logger logger = LogManager.getLogger(TestBackupCmd.class);
	
	public String getResourceSubFolder(){
		return "backup/";
	}
	
	@Test
	public void test1() throws Exception{
		String allFolder = "/test/BackupCmd/data/allFolder1/";
		String wfidFolder = "/test/BackupCmd/data/wfidFolder1/";

		String staticCfgName = "backup_test1_staticCfg.properties";
		String wfName="wfid1";
		String wfid="wfid1";

		String[] allFiles = new String[]{"all1", "all2"};
		String[] wfidFiles = new String[]{wfid+"/a", wfid+"/b", "a", wfid+"abcd"};

		String localFile = "backup_test1_data";
		String historyFolder = "/test/datahistory/";
		//generate all the data files
		getFs().delete(new Path(allFolder), true);
		getFs().delete(new Path(wfidFolder), true);
		
		for (String allFile: allFiles){
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localFile), new Path(allFolder + allFile));
		}
		for (String wfidFile: wfidFiles){
			getFs().copyFromLocalFile(new Path(getLocalFolder() + localFile), new Path(wfidFolder + wfidFile));
		}
		//run cmd
		BackupCmd cmd = new BackupCmd(wfName, wfid, getResourceSubFolder() + staticCfgName, getDefaultFS(), null);
		List<String> info = cmd.sgProcess();
		int numFiles = Integer.parseInt(info.get(0));
		logger.info(String.format("%d files backedup", numFiles));
		assertTrue(numFiles==3);
		//check results
		List<String> flist;
		//allFolder should be empty
		flist = HdfsUtil.listDfsFile(getFs(), allFolder);
		assertTrue(flist.size()==0);
		//check the zip file
		String ZipFileName=wfid+".zip";
		flist = HdfsUtil.listDfsFile(getFs(), historyFolder);
		assertTrue(flist.contains(ZipFileName));
		//Check the number of files in Zip
		int filecount=Util.getZipFileCount(getFs(),historyFolder+ZipFileName);
		assertTrue(filecount==3);
			
	}
}