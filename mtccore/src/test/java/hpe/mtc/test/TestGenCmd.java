package hpe.mtc.test;

import org.junit.Test;

import hpe.mtc.GenCmd;

public class TestGenCmd {
	
	//public static String working_dir="C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\test\\resources\\";
	public static String working_dir="/Users/chengyi/git/log.analysis/mtccore/src/test/resources/";
	@Test
	public void test1(){
		GenCmd.main(new String[]{"-i", working_dir+"sgsiwf.xml", "-o", working_dir, "-p", "sample1"});
	}
	
	//
	@Test
	public void testMergedTable(){
		GenCmd.main(new String[]{"-i", working_dir+"sgsiwftest1.xml", "-o", working_dir, "-p", "test1"});
	}
	
	@Test
	public void testGenDataFieldOrder(){
		
	}

}
