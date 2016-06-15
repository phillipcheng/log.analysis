package etl.util;

import java.util.Date;
import java.util.Map;


import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.log4j.Logger;


public class ScriptEngineUtil {
	public static final Logger logger = Logger.getLogger(ScriptEngineUtil.class);
	
	private static ScriptEngineManager manager = new ScriptEngineManager();
	
	public static Object eval(String exp, VarType toType, Map<String,Object> variables){
		return eval(exp, toType, variables, true);
	}
	public static Object eval(String exp, VarType toType, Map<String,Object> variables, boolean logError){
		ScriptEngine jsEngine = manager.getEngineByName("JavaScript");
		if (variables!=null){
			for (String key: variables.keySet()){
				Object v = variables.get(key);
				if (v instanceof Date){
					jsEngine.put(key, ((Date)v).getTime());
				}else{
					jsEngine.put(key, v);
				}
			}
		}
		try {
			Object ret = jsEngine.eval(exp);
			logger.debug(String.format("eval %s get result %s", exp, ret));
			if (toType == VarType.OBJECT){
				return ret;
			}
			if (ret!=null){
				if (ret instanceof String){
					if (toType == VarType.STRING){
						;
					}else if (toType == VarType.INT){
						ret = Integer.parseInt((String)ret);
					}else{
						logger.error(String.format("unsupported to type for string result: %s", toType));
					}
				}else if (ret instanceof Double){
					if (toType ==VarType.INT){
						ret = ((Double)ret).intValue();
					}else if (toType==VarType.FLOAT){
						ret = ((Double)ret).floatValue();
					}else{
						logger.error(String.format("unsupported to type for double result: %s", toType));
					}
				}else if (ret instanceof Boolean){
					if (toType==VarType.BOOLEAN){
						return ret;
					}else{
						logger.error(String.format("expect a boolean result from exp:%s", exp));
						return null;
					}
				}else{
					logger.error(String.format("unsupported type of eval ret: %s", ret.getClass()));
				}
			}
			return ret;
		} catch (ScriptException e) {
			if (logError){
				logger.error(String.format("error msg: %s while eval %s, var map is %s", e.getMessage(), exp, variables));
			}
			return null;
		}
	}
}
