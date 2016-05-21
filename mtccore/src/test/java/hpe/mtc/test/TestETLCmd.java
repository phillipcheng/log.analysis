package hpe.mtc.test;

import org.junit.Test;

import hpe.mtc.ETLCmd;

public class TestETLCmd {
	
	public static String working_dir="C:\\mydoc\\myprojects\\log.analysis\\mtccore\\working\\";
	//public static String working_dir="/Users/chengyi/git/log.analysis/mtccore/src/test/resources/";
	@Test
	public void test1(){
		ETLCmd.main(new String[]{"-c", "sgsiwf.detl.properties", "-x", working_dir+"xmldata\\"});
	}
	
	@Test
	public void testAddTables(){
		ETLCmd.main(new String[]{"-c", "sgsiwf.detl.properties", "-x", working_dir+"xmldata1\\"});
		ETLCmd.main(new String[]{"-c", "sgsiwf.detl.properties", "-x", working_dir+"xmldata2\\"});
	}
	
	@Test
	public void testMergeTable(){
		ETLCmd.main(new String[]{"-c", "sgsiwf.detl.properties", "-x", working_dir+"xmldata3\\"});
		ETLCmd.main(new String[]{"-c", "sgsiwf.detl.properties", "-x", working_dir+"xmldata4\\"});
	}
	
	@Test
	public void testGenDataOnly(){
		ETLCmd.main(new String[]{"-c", "sgsiwf.detl.properties", "-x", working_dir+"xmldata\\"});
		ETLCmd.main(new String[]{"-c", "sgsiwf.detl.properties", "-x", working_dir+"xmldata\\"});
	}

}
