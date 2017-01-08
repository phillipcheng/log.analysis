package etl.util;

import java.util.Date;
import java.util.Map;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ScriptEngineUtil {
	public static final Logger logger = LogManager.getLogger(ScriptEngineUtil.class);
	
	public static String ListSeparator=",";
	
	private static ScriptEngineManager manager = new ScriptEngineManager();
	
	public static Object eval(String exp, VarType toType, Map<String,Object> variables){
		return eval(exp, toType, variables, true);
	}
	
	public static CompiledScript compileScript(String exp){
		ScriptEngine jsEngine = manager.getEngineByName("nashorn");
		Compilable compEngine = (Compilable)jsEngine;
        CompiledScript cs = null;
		try {
			cs = compEngine.compile(exp);
		} catch (ScriptException e) {
			logger.error("", e);
		}
		return cs;
	}
	
	public static String eval(CompiledScript cs, Map<String, Object> variables){
		Bindings bindings = new SimpleBindings();
        if (variables!=null){
			for (String key: variables.keySet()){
				Object v = variables.get(key);
				if (v instanceof Date){
					bindings.put(key, ((Date)v).getTime());
				}else{
					bindings.put(key, v);
				}
			}
		}
		try {
			Object ret = cs.eval(bindings);
			logger.debug(String.format("eval get result: '%s'", ret));
			if (ret!=null){
				if (ret instanceof String){
					return (String) ret;
				}else if (ret instanceof Double){
					return ((Double)ret).toString();
				}else if (ret instanceof String[]){
					String[] sa = (String[]) ret;
					return String.join(ListSeparator, sa);
				}else{
					logger.error(String.format("unsupported type of eval ret: %s", ret.getClass()));
				}
			}
			return null;
		} catch (Exception e) {
			logger.error(String.format("error msg: %s while eval %s, var map is %s", e.getMessage(), cs, variables), e);
			return null;
		}
	}
	
	public static Object evalObject(CompiledScript cs, Map<String, Object> variables){
		return evalObject(cs, null, variables);
	}
	
	public static Object evalObject(CompiledScript cs, String orgExp, Map<String, Object> variables){
		Bindings bindings = new SimpleBindings();
        if (variables!=null){
			for (String key: variables.keySet()){
				Object v = variables.get(key);
				if (v instanceof Date){
					bindings.put(key, ((Date)v).getTime());
				}else{
					bindings.put(key, v);
				}
			}
		}
		try {
			Object ret = cs.eval(bindings);
			logger.debug(String.format("eval get result: '%s'", ret));
			return ret;
		} catch (Exception e) {
			logger.error(String.format("error msg: %s while eval %s, orgExp: %s, var map is %s", e.getMessage(), cs, orgExp, variables));
			return null;
		}
	}
	
	public static Object eval(String exp, VarType toType, Map<String,Object> variables, boolean logError){
		ScriptEngine jsEngine = manager.getEngineByName("nashorn");
		
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
				}else if (ret instanceof String[]){
					String[] sa = (String[]) ret;
					return String.join(ListSeparator, sa);
				}
				else{
					logger.error(String.format("unsupported type of eval ret: %s", ret.getClass()));
				}
			}
			return ret;
		} catch (Exception e) {
			if (logError){
				logger.error(String.format("error msg: %s while eval %s, var map is %s", e.getMessage(), exp, variables));
			}
			return null;
		}
	}
}
