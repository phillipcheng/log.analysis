package bdap.schemagen.cmd.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import bdap.schemagen.cmd.StatSchemaGenerateCmd;
import etl.cmd.test.TestETLCmd;



public class TestStatSchemaGenerateCmd extends TestETLCmd{
	
	private static final String cmdClassName = "bdap.schemagen.cmd.StatSchemaGenerateCmd";

	@Test
	public void testSgProcess() throws Exception{
		String staticCfgName = "conf.properties";
		String wfid="wfid1";
		String inputFolder = "/etltest/statsg/input/";
		String cfgFolder = "/etltest/statsg/cfg/";
//		String[] csvFileNames = new String[]{"STAT1027_AJ2","STAT1027_BBG2","STAT1027_CH2","STAT1027_SLK2","STAT1028_AJ2","STAT1028_BBG2","STAT1028_CH2"};
		String[] csvFileNames = new String[]{"STAT1027_AJ2"};
		String schemaFileName = "schema.ls";
			
		getFs().delete(new Path(inputFolder), true);
		getFs().mkdirs(new Path(inputFolder));
		getFs().delete(new Path(cfgFolder), true);
		getFs().mkdirs(new Path(cfgFolder));
		
		
		getFs().setPermission(new Path(inputFolder), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
		
		//copy csv file
		for(String csvFile:csvFileNames){
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile), new Path(inputFolder + csvFile));
			getFs().setPermission(new Path(inputFolder + csvFile), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
		}
		
		//copy schema file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFileName), new Path(cfgFolder + schemaFileName));
		
		
		StatSchemaGenerateCmd ssgcmd=new StatSchemaGenerateCmd("wf1", wfid, this.getResourceSubFolder() + staticCfgName, getDefaultFS(), null);			
		ssgcmd.sgProcess();
	}

	@Override
	public String getResourceSubFolder() {
		return "statsg/";
	}
}
