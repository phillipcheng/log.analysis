package hpe.mtc.test;

import org.junit.Test;

import hpe.mtc.GenCmd;

public class TestGenCmd {
	
	@Test
	public void test1(){
		GenCmd.main(new String[]{"-i", "C:\\projects\\TBDA\\Verizon\\mtccore\\src\\test\\resources\\sgsiwf.xml", 
				"-o", "C:\\projects\\TBDA\\Verizon\\mtccore\\src\\test\\resources\\", });
	}
	
	@Test
	public void test2(){
		GenCmd.main(new String[]{"-i", "C:\\projects\\TBDA\\Verizon\\mtccore\\src\\test\\resources\\sgsiwftest1.xml", 
				"-o", "C:\\projects\\TBDA\\Verizon\\mtccore\\src\\test\\resources\\", });
	}

}
