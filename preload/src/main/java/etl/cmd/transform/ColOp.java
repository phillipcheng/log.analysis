package etl.cmd.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class ColOp {
	public static final Logger logger = Logger.getLogger(ColOp.class);
	
	public static final String UPDATE_OP="u";
	public static final String REMOVE_OP="r";
	public static final String SPLIT_OP="s";

	public static final String VAR_NAME_fields="fields";
	
	private ColOpType type;
	private int targetIdx;
	private String exp;
	
	public int getTargetIdx() {
		return targetIdx;
	}
	public void setTargetIdx(int targetIdx) {
		this.targetIdx = targetIdx;
	}
	public String getExp() {
		return exp;
	}
	public void setExp(String exp) {
		this.exp = exp;
	}
	
	public ColOp(String str){
		String[] strs = str.split("\\|");
		if (strs.length==2){
			String op = strs[0];
			if (UPDATE_OP.equals(op)){
				type = ColOpType.update;
			}else if (REMOVE_OP.equals(op)){
				type = ColOpType.remove;
			}else if (SPLIT_OP.equals(op)){
				type = ColOpType.split;
			}else{
				logger.error(String.format("wrong col op type:%s", op));
			}
			String idxexp = strs[1];
			String[] ies = idxexp.split("\\:");
			if (ies.length>=2){
				exp = ies[1];
			}
			if (ies.length>=1){
				targetIdx = Integer.parseInt(ies[0]);
			}
		}else{
			logger.error(String.format("expected format: op|idx:exp, %s", str));
		}
	}
	
	public List<String> process(Map<String, Object> vars){
		List<String> items = new ArrayList<String>();
		items.addAll(Arrays.asList((String[]) vars.get(VAR_NAME_fields)));
		if (type == ColOpType.update){
			String val = (String) ScriptEngineUtil.eval(exp, VarType.STRING, vars);
			for (int i=items.size(); i<=targetIdx; i++){
				items.add("");//fill empty string
			}
			items.set(targetIdx, val);
		}else if (type == ColOpType.remove){
			items.remove(targetIdx);
		}else if (type == ColOpType.split){
			String item = items.get(targetIdx);
			String[] newItem = item.split(Pattern.quote(exp));
			items.remove(targetIdx);
			items.addAll(targetIdx, Arrays.asList(newItem));
		}
		return items;
	}
}
