package etl.engine.test;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
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
	
	@Test
	public void testDtStandardize(){
		String input = " 20160409.1412 ";
		String output = GroupFun.dtStandardize(input, "yyyyMMdd.HHmm");
		logger.info(output);
		Assert.assertEquals(output, "2016-04-09 14:12:00.000");
		
		output = GroupFun.dtStandardize("", "yyyyMMdd.HHmm");
		logger.info(output);
		Assert.assertEquals(output, "");
	}
	
	
	@Test
	public void testSplitTimeRange() throws Exception{
		
		String[] value=GroupFun.splitTimeRange("2016-10-12 10:10:02", "2016-10-12 10:11:02", "yyyy-MM-dd HH:mm:ss", "UTC", 5000L);
		
		String readableValue=String.join("\n", value);
		logger.info("Value:\n{}", readableValue);
		
		Assert.assertEquals(13, value.length);
		Assert.assertEquals("2016-10-12 10:10:00,2016-10-12 10:10:05", value[0]);
		Assert.assertEquals("2016-10-12 10:11:00,2016-10-12 10:11:05", value[12]);		
	}
}
