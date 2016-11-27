package etl.cmd.test;

import org.junit.Assert;
import org.junit.Test;

import etl.cmd.SQLExecutionCmd;
import etl.util.DBUtil;


public class TestSQLExecutionCmd extends TestETLCmd{
	
	private static final String cmdClassName = "etl.cmd.SQLExecutionCmd";

	@Test
	public void testSgProcess() throws Exception{
		String staticCfgName = "conf.properties";
		String wfid="wfid1";
		
		SQLExecutionCmd sqlExecCmd=new SQLExecutionCmd("wf1", wfid, this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);			
		sqlExecCmd.sgProcess();
		
	}
	

	@Override
	public String getResourceSubFolder() {
		return "sqlexec/";
	}

}
