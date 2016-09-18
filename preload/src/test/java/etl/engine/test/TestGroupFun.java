package etl.engine.test;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.util.GroupFun;

public class TestGroupFun {
	
	public static final Logger logger = LogManager.getLogger(TestGroupFun.class);
	
	@Test
	public void testSetDate(){
		String input = "1460678400";
		String output = GroupFun.changeDateToCurrent(input);
		
		String dayInput = GroupFun.dayEpoch(input);
		String hourInput = GroupFun.hourEpoch(input);
		logger.info(String.format("day:%s, hour:%s", dayInput, hourInput));
		String dayOutput = GroupFun.dayEpoch(output);
		String hourOutput = GroupFun.hourEpoch(output);
		logger.info(String.format("day:%s, hour:%s", dayOutput, hourOutput));
		
	}

}
