package bdap.util.test;

import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.Util;

public class TestUtil {
	public static final Logger logger = LogManager.getLogger(TestUtil.class);
	@Test
	public void testSplitBatch1(){
		List<int[]> ret = Util.createBatch(50, 402);
		for (int[] pair:ret){
			logger.info(Arrays.toString(pair));
		}
	}
	
	@Test
	public void testSplitBatch2(){
		List<int[]> ret = Util.createBatch(50, 40);
		for (int[] pair:ret){
			logger.info(Arrays.toString(pair));
		}
	}
	
	@Test
	public void testSplitBatch3(){
		List<int[]> ret = Util.createBatch(50, 50);
		for (int[] pair:ret){
			logger.info(Arrays.toString(pair));
		}
	}

}
