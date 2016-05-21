package hpe.mtc.test;

import org.junit.Test;

import hpe.mtc.ETLCmd;

public class TestETLCmd {
	
	public static String working_dir="C:\\mydoc\\myprojects\\log.analysis\\mtccore\\working\\";
	//public static String working_dir="/Users/chengyi/git/log.analysis/mtccore/src/test/resources/";
	@Test
	public void test1(){
		ETLCmd.main(new String[]{"-x", working_dir+"xmldata\\", "-c", working_dir+"csvdata\\", "-s", working_dir+"schema\\", 
				"-m", working_dir+"schemahistory\\", "-d", working_dir+"datahistory\\", "-p", "sgsiwf"});
	}
	
	@Test
	public void testAddTables(){
		ETLCmd.main(new String[]{"-x", working_dir+"xmldata1\\", "-c", working_dir+"csvdata\\", "-s", working_dir+"schema\\", 
				"-m", working_dir+"schemahistory\\", "-d", working_dir+"datahistory\\", "-p", "sgsiwf"});
		ETLCmd.main(new String[]{"-x", working_dir+"xmldata2\\", "-c", working_dir+"csvdata\\", "-s", working_dir+"schema\\", 
				"-m", working_dir+"schemahistory\\", "-d", working_dir+"datahistory\\", "-p", "sgsiwf"});
	}
	
	@Test
	public void testMergeTable(){
		ETLCmd.main(new String[]{"-x", working_dir+"xmldata3\\", "-c", working_dir+"csvdata\\", "-s", working_dir+"schema\\", 
				"-m", working_dir+"schemahistory\\", "-d", working_dir+"datahistory\\", "-p", "sgsiwf"});
		ETLCmd.main(new String[]{"-x", working_dir+"xmldata4\\", "-c", working_dir+"csvdata\\", "-s", working_dir+"schema\\", 
				"-m", working_dir+"schemahistory\\", "-d", working_dir+"datahistory\\", "-p", "sgsiwf"});
	}
	
	@Test
	public void testGenDataOnly(){
		ETLCmd.main(new String[]{"-x", working_dir+"xmldata1\\", "-c", working_dir+"csvdata\\", "-s", working_dir+"schema\\", 
				"-m", working_dir+"schemahistory\\", "-d", working_dir+"datahistory\\", "-p", "sgsiwf"});
		ETLCmd.main(new String[]{"-x", working_dir+"xmldata1\\", "-c", working_dir+"csvdata\\", "-s", working_dir+"schema\\", 
				"-m", working_dir+"schemahistory\\", "-d", working_dir+"datahistory\\", "-p", "sgsiwf"});
	}

}
