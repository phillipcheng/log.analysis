package etl.engine.test;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.cmd.CsvFileGenCmd;
import etl.cmd.test.TestETLCmd;
import etl.engine.ETLCmdMain;
import etl.util.Util;

public class TestExeIntervalCmd extends TestETLCmd{
	public static final Logger logger = LogManager.getLogger(TestExeIntervalCmd.class);
	
	public String getResourceSubFolder(){
		return "csvgen/";
	}
	
	public void testRunForever(){
		try{
			//
			String remoteCfgFolder = "/test/csvgen/cfg/";
			String remoteSchemaFolder = "/test/csvgen/schema/";
			String staticCfg = "csvgen1.properties";
			String schemaFile = "schema1.txt";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			
			getFs().delete(new Path(remoteSchemaFolder), true);
			getFs().mkdirs(new Path(remoteSchemaFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteSchemaFolder + schemaFile));
			
			CsvFileGenCmd cmd = new CsvFileGenCmd("wf1", "wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			
			getFs().delete(new Path(cmd.getOutputFolder()), true);
			getFs().mkdirs(new Path(cmd.getOutputFolder()));
			
			ETLCmdMain.exeCmd(cmd, 10, 0);
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testRunlimitedTimes(){
		try{
			//
			String remoteCfgFolder = "/test/csvgen/cfg/";
			String remoteSchemaFolder = "/test/csvgen/schema/";
			String staticCfg = "csvgen1.properties";
			String schemaFile = "schema1.txt";
			
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticCfg), new Path(remoteCfgFolder + staticCfg));
			
			getFs().delete(new Path(remoteSchemaFolder), true);
			getFs().mkdirs(new Path(remoteSchemaFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFile), new Path(remoteSchemaFolder + schemaFile));
			
			CsvFileGenCmd cmd = new CsvFileGenCmd("wf1", "wf1", remoteCfgFolder + staticCfg, getDefaultFS(), null);
			
			getFs().delete(new Path(cmd.getOutputFolder()), true);
			getFs().mkdirs(new Path(cmd.getOutputFolder()));
			
			ETLCmdMain.exeCmd(cmd, 4, 10);
			//assertion
			List<String> ret = Util.listDfsFile(cmd.getFs(), cmd.getOutputFolder());
			assertTrue(ret.size()>=1);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
