package hpe.mtc.test;

import org.junit.Test;

import hpe.mtc.GenCmd;

public class TestGenCmd {
	
	@Test
	public void test1(){
		GenCmd.main(new String[]{"-i", "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\test\\resources\\sgsiwf.xml", 
				"-o", "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\test\\resources\\", "-p", "sample1"});
	}
	
	//
	@Test
	public void testMergedTable(){
		GenCmd.main(new String[]{"-i", "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\test\\resources\\sgsiwftest1.xml", 
				"-o", "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\test\\resources\\", "-p", "test1"});
	}
	
	@Test
	public void testGenDataFieldOrder(){
		
	}

}
