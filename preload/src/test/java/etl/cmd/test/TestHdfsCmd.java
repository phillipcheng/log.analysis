package etl.cmd.test;

import java.util.List;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import etl.cmd.HdfsCmd;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestHdfsCmd extends TestETLCmd{

	public static final Logger logger = LogManager.getLogger(TestHdfsCmd.class);
	
	public String getResourceSubFolder(){
		return "hdfscmd/";
	}
	
	@Test
	public void test1Fun() throws Exception{
		String wfName = "wfName";
		String wfId = "wfid";
		String cfg = "hdfscmd1.properties";
		String dfsCfgFolder = "/test/hdfscmd/cfg/";
		

		getFs().delete(new Path(dfsCfgFolder), true);
		getFs().mkdirs(new Path(dfsCfgFolder));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsCfgFolder + cfg));
		
		HdfsCmd cmd = new HdfsCmd(wfName, wfId, dfsCfgFolder + cfg, null, super.getDefaultFS(), null);
		String[] folders = cmd.getRmFolders();
		for (String f:folders){
			super.getFs().mkdirs(new Path(f));
		}
		List<String> info = cmd.sgProcess();
		
		logger.info(info);
	}
}