package etl.cmd.test;

import static org.junit.Assert.assertTrue;

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
	public void test1Fun() {
		try {
			String wfName = "wfName";
			String wfId = "wfid";
			String cfg = "hdfscmd1.properties";
			
			HdfsCmd cmd = new HdfsCmd(wfName, wfId, this.getResourceSubFolder() + cfg, null, super.getDefaultFS(), null);
			String[] folders = cmd.getRmFolders();
			for (String f:folders){
				super.getFs().mkdirs(new Path(f));
			}
			List<String> info = cmd.sgProcess();
			
			logger.info(info);
		}catch(Exception e){
			logger.error("", e);
			assertTrue(false);
		}
	}
}