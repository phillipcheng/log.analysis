package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import bdap.util.HdfsUtil;
import bdap.util.SftpInfo;
import bdap.util.SftpUtil;
import etl.cmd.HousekeepingCmd;
import etl.cmd.SftpCmd;
import etl.engine.EngineUtil;

public class TestHousekeepingCmd extends TestETLCmd{

	public TestHousekeepingCmd() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getResourceSubFolder() {
		// TODO Auto-generated method stub
		return "housekeeping"+File.separator;
	}
	
	
	@Test
	public void sgprocess() throws Exception{
		String cfg = "housekeeping.properties";
		
		HousekeepingCmd cmd = new HousekeepingCmd("wf1", null, this.getResourceSubFolder() + cfg, getDefaultFS(), null);

		List<String> list = cmd.sgProcess();

		System.out.println("HDFS File:\n" + list.toString());
		assertTrue(list.size()!=1);
		
	}
	
	

}
