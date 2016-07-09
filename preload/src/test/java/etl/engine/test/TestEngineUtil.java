package etl.engine.test;

import java.util.Date;

import org.junit.Test;

import etl.engine.ETLLog;
import etl.engine.EngineUtil;

public class TestEngineUtil {
	
	private ETLLog getETLLog(){
		ETLLog etllog = new ETLLog();
		etllog.setActionName("test");
		etllog.setStart(new Date());
		etllog.setEnd(new Date());
		return etllog;
	}
	
	@Test
	public void testNormalSend(){
		EngineUtil.setConfFile("etlengine_enable_kafka.properties");
		EngineUtil.getInstance().sendLog(getETLLog());
		EngineUtil.getInstance().getProducer().flush();
	}
	
	@Test
	public void testNotSend(){
		EngineUtil.setConfFile("etlengine.properties");
		EngineUtil.getInstance().sendLog(getETLLog());
	}
	
	@Test
	public void testAbnormalSend(){
		EngineUtil.setConfFile("etlengine_bad_kafka.properties");
		EngineUtil.getInstance().sendLog(getETLLog());
	}

}
