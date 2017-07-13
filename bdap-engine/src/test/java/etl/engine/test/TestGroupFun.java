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
	public void testConvertTimeStampToString(){
		String input = " 1424820819999  ";
		String fomrmat = "yyyy-MM-dd HH:mm:ss";
		String output = GroupFun.convertTimeStampToString(input,fomrmat,null);
		logger.info(output);
		Assert.assertEquals(output, "2015-02-24 23:33:39");
		output = GroupFun.convertTimeStampToString("",fomrmat,null);
		logger.info(output);
		Assert.assertEquals(output, "");
	}
	
	@Test
	public void testSplitTimeRange() throws Exception{
		
		String[] value=GroupFun.splitTimeRange("2016-10-12 10:10:02", "2016-10-12 10:10:05", "yyyy-MM-dd HH:mm:ss", "UTC","yyyy-MM-dd HH:mm:ss.S",5000L);
		
		String readableValue=String.join("\n", value);
		logger.info("Value:\n{}", readableValue);
		
		Assert.assertEquals(2, value.length);
		Assert.assertEquals("2016-10-12 10:10:00.0,2016-10-12 10:10:04.999", value[0]);
		Assert.assertEquals("2016-10-12 10:10:05.0,2016-10-12 10:10:09.999", value[1]);
	}
	
	@Test
	public void testspiltbysinglespace() throws Exception{
		
		String value=GroupFun.spiltbysinglespace("5 1289755 707652 582103 ");
		
		Assert.assertEquals("5,1289755,707652,582103", value);
	}

}
