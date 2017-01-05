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
	public void testRm() throws Exception{
		String wfName = "wfName";
		String wfId = "wfid";
		String cfg = "hdfsrm.properties";
		
		HdfsCmd cmd = new HdfsCmd(wfName, wfId, this.getResourceSubFolder() + cfg, null, super.getDefaultFS(), null);
		String[] folders = cmd.getRmFolders();
		for (String f:folders){
			getFs().mkdirs(new Path(f));
		}
		cmd.sgProcess();
		
		//assertion
		for (String f:folders){
			assertTrue(!getFs().exists(new Path(f)));
		}
	}
	
	@Test
	public void testMapreduceMv() throws Exception{	
		throw new UnsupportedOperationException();
	}
	
	@Test
	public void testMapreduceRmFiles() throws Exception{
		throw new UnsupportedOperationException();
	}
	
	@Test
	public void testMapreduceRmParents() throws Exception{
		throw new UnsupportedOperationException();
	}
	
	@Test
	public void testMvDir() throws Exception{
		String wfName = "wfName";
		String wfId = "wfid";
		String cfg = "hdfsMvDir.properties";
		
		HdfsCmd cmd = new HdfsCmd(wfName, wfId, this.getResourceSubFolder() + cfg, null, super.getDefaultFS(), null);
		String[] fromFolders = cmd.getMvFrom();
		String[] toFolders = cmd.getMvTo();
		for (String f:fromFolders){
			super.getFs().mkdirs(new Path(f));
		}
		cmd.sgProcess();
		//assertion
		for (String f:fromFolders){
			assertTrue(!getFs().exists(new Path(f)));
		}
		for (String f:toFolders){
			assertTrue(getFs().exists(new Path(f)));
		}
	}
	
	@Test
	public void testMvFile() throws Exception{
		String wfName = "wfName";
		String wfId = "wfid";
		String cfg = "hdfsMvFile.properties";
		
		HdfsCmd cmd = new HdfsCmd(wfName, wfId, this.getResourceSubFolder() + cfg, null, super.getDefaultFS(), null);
		String[] fromFiles = cmd.getMvFrom();
		String[] toFiles = cmd.getMvTo();
		for (String f:fromFiles){
			getFs().copyFromLocalFile(new Path(this.getLocalFolder()+"abc.txt"), new Path(f));
		}
		for (String f:toFiles){
			if (getFs().exists(new Path(f))){
				getFs().delete(new Path(f), true);
			}
		}
		cmd.sgProcess();
		//assertion
		for (String f:fromFiles){
			assertTrue(!getFs().exists(new Path(f)));
		}
		for (String f:toFiles){
			assertTrue(getFs().exists(new Path(f)));
		}
	}
	
	@Test
	public void testMkdir() throws Exception{
		String wfName = "wfName";
		String wfId = "wfid";
		String cfg = "hdfsMkdir.properties";
		
		HdfsCmd cmd = new HdfsCmd(wfName, wfId, this.getResourceSubFolder() + cfg, null, super.getDefaultFS(), null);
		String[] mkdirFolders = cmd.getMkdirFolders();
		for (String f:mkdirFolders){
			if (getFs().exists(new Path(f))){
				getFs().delete(new Path(f), true);
			}
		}
		cmd.sgProcess();
		//assertion
		for (String f:mkdirFolders){
			getFs().exists(new Path(f));
			//how to check permission //TODO
		}
	}
}