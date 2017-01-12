package etl.engine.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.SimpleBindings;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.cmd.test.TestETLCmd;
import etl.engine.EngineUtil;
import etl.log.ETLLog;
import etl.log.LogType;
import etl.util.DateUtil;
import etl.util.GroupFun;
import etl.util.ScriptEngineUtil;
import etl.util.StringUtil;
import etl.util.VarType;
import junit.framework.Assert;

public class TestEngineUtil extends TestETLCmd{
	public static final Logger logger = LogManager.getLogger(TestEngineUtil.class);
	private ETLLog getETLLog(){
		ETLLog etllog = new ETLLog(LogType.etlstat);
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
	public void testJsEnginePerformance(){
		String exp="'STAT_GROUP_'+originTableName";
		String exp2="originTableName+'_'+filename.substring(filename.lastIndexOf('_')+1)";
		CompiledScript cs=ScriptEngineUtil.compileScript(exp);
		CompiledScript cs2=ScriptEngineUtil.compileScript(exp2);
		
		long startTime=System.currentTimeMillis();
		long c=100000l;

		for(long i=0;i<c;i++){
			String value=String.valueOf((int)(Math.random()*100));
			Map<String,Object> var=new HashMap<String,Object>();
			var.put("originTableName", value);
			var.put("filename", value+"_BBG");
			String ret=ScriptEngineUtil.eval(cs, var);
			var.put("value", ret);
			ret=ScriptEngineUtil.eval(cs2, var);
//			System.out.println(ret);
//			assertEquals(value, ret);
		}
		long endTime=System.currentTimeMillis();
		long duration=endTime-startTime;
		double latency=((double)duration)/((double)c);
		System.out.println(String.format("Duration:%s, Latency:%s", duration, latency));
		
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

	@Test
	public void testGlobToRegExp(){
		String a = StringUtil.convertGlobToRegEx("/test/a/*-part*");
		logger.info(a);
	}
	@Override
	public String getResourceSubFolder() {
		return "engineutil/";
	}
	
	@Test
	public void testGetParentFolderName(){
		assertTrue("site1".equals(GroupFun.getParentFolderName("hdfs://127.0.0.1/pde/ssxxxxx-xx/csv/site1/xxxx.bin")));
	}
	
	
}
