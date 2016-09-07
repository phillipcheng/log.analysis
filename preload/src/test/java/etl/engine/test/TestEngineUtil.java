package etl.engine.test;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.test.TestETLCmd;
import etl.engine.ETLLog;
import etl.engine.EngineUtil;
import etl.util.DateUtil;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class TestEngineUtil extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(TestEngineUtil.class);
	private ETLLog getETLLog(){
		ETLLog etllog = new ETLLog();
		etllog.setActionName("test");
		etllog.setStart(new Date());
		etllog.setEnd(new Date());
		return etllog;
	}
	
	@Test
	public void testNormalSend(){
		if (!super.isTestKafka()) return;
		EngineUtil.setConfFile("etlengine_enable_kafka.properties");
		EngineUtil.getInstance().sendLog(getETLLog());
	}
	
	@Test
	public void testNotSend(){
		if (!super.isTestKafka()) return;
		EngineUtil.setConfFile("etlengine.properties");
		EngineUtil.getInstance().sendLog(getETLLog());
	}
	
	@Test
	public void testAbnormalSend(){
		if (!super.isTestKafka()) return;
		EngineUtil.setConfFile("etlengine_bad_kafka.properties");
		EngineUtil.getInstance().sendLog(getETLLog());
	}
	
	
	@Test
	public void testJsEngineConstant(){
		Map<String, Object> vars = new HashMap<String, Object>();
		String exp = "'/pde/loadcsv'";
		String output = (String) ScriptEngineUtil.eval(exp, VarType.STRING, vars);
		assertTrue("/pde/loadcsv".equals(output));
		logger.info(output);
	}
	
	@Test
	public void testJsEngineInvokeJava(){
		Map<String, Object> vars = new HashMap<String, Object>();
		String exp = "var telecomUtil = Java.type(\"etl.telecom.TelecomUtil\"); telecomUtil.processE164('1234');";
		String output = (String) ScriptEngineUtil.eval(exp, VarType.STRING, vars);
		logger.info(output);
	}
	
	@Test
	public void testJsEnginePassingMap(){
		Map<String, Object> vars = new HashMap<String, Object>();
		Map<String, Object> dynCfgMap = new HashMap<String, Object>();
		dynCfgMap.put("abcString", "abcValue");
		String[] abcArray = new String[]{"abcArray1", "abcArray2"};
		dynCfgMap.put("abcArray", abcArray);
		vars.put("dynCfgMap", dynCfgMap);
		String exp = "dynCfgMap['abcString'];";
		String output = (String) ScriptEngineUtil.eval(exp, VarType.STRING, vars);
		assertTrue("abcValue".equals(output));
		exp = "dynCfgMap['abcArray'];";
		output = (String) ScriptEngineUtil.eval(exp, VarType.STRING, vars);
		logger.info(output);
	}
	
	@Test
	public void testDayOfWeekYesterday(){
		String str = DateUtil.getWeekOfDayForYesterday();
		logger.info(str);
	}

	@Override
	public String getResourceSubFolder() {
		return "engineutil/";
	}
}
