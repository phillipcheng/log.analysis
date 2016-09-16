package etl.cmd.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.script.CompiledScript;

import org.apache.log4j.Logger;

import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class ColOp {
	public static final Logger logger = Logger.getLogger(ColOp.class);
	
	public static final String UPDATE_OP="u";
	public static final String REMOVE_OP="r";
	public static final String SPLIT_OP="s";

	public static final String VAR_NAME_FIELDS="fields"; //array of field values
	public static final String VAR_NAME_FIELD_MAP="fieldMap"; //field name to string value map
	
	private ColOpType type;
	private int targetIdx;
	private String exp;
	private CompiledScript expCS;
	
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
	
	public ColOp(String str, Map<String, Integer> attrMap){
		String[] strs = str.split("\\|", -1);
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
			String[] ies = idxexp.split("\\:", 2);
			if (ies.length==2){
				exp = ies[1];
				if (type == ColOpType.update){
					expCS = ScriptEngineUtil.compileScript(exp);
				}
			}
			if (ies.length>=1){
				if (attrMap==null){
					targetIdx = Integer.parseInt(ies[0]);
				}else{
					String targetAttr = ies[0];
					if (attrMap.containsKey(targetAttr)){
						targetIdx = attrMap.get(targetAttr);
					}else{
						logger.error(String.format("attrMap %s does not contains target %s", attrMap, targetAttr));
					}
				}
			}
		}else{
			logger.error(String.format("expected format: op|idx:exp, %s", str));
		}
	}
	
	public List<String> process(Map<String, Object> vars){
		List<String> items = new ArrayList<String>();
		items.addAll(Arrays.asList((String[]) vars.get(VAR_NAME_FIELDS)));
		if (type == ColOpType.update){
			String val = ScriptEngineUtil.eval(expCS, vars);
			if (val!=null){
				for (int i=items.size(); i<=targetIdx; i++){
					items.add("");//fill empty string
				}
				if (val.contains(",")){//may return a list of string from update
					String[] newItems = val.split(",", -1);
					items.remove(targetIdx);
					items.addAll(targetIdx, Arrays.asList(newItems));
				}else{
					items.set(targetIdx, val);
				}
			}else{
				logger.error(String.format("%s evaluated to null with vars %s.", expCS, vars));
			}
		}else if (type == ColOpType.remove){
			items.remove(targetIdx);
		}else if (type == ColOpType.split){
			String item = items.get(targetIdx);
			String[] newItem = item.split(Pattern.quote(exp), -1);
			items.remove(targetIdx);
			items.addAll(targetIdx, Arrays.asList(newItem));
		}
		return items;
	}
}
