package etl.cmd.transform;

import java.util.HashMap;
import java.util.Map;

import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class ColUpdate {
	public static final String COMMAND="update.idx";
	public static final String VAR_NAME="a";
	
	private int idx;
	private String exp;//operation expression
	
	
	public ColUpdate(int idx, String exp){
		this.idx = idx;
		this.setExp(exp);
	}
	
	public int getIdx() {
		return idx;
	}

	public void setIdx(int idx) {
		this.idx = idx;
	}

	public String getExp() {
		return exp;
	}

	public void setExp(String exp) {
		this.exp = exp;
	}
	
	public String process(String item){
		Map<String, Object> vars = new HashMap<String, Object>();
		vars.put(VAR_NAME, item);
		return (String) ScriptEngineUtil.eval(exp, VarType.STRING, vars);
	}
}
