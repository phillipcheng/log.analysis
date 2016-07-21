package etl.cmd.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import etl.engine.AggrOperator;
import etl.util.IdxRange;

public class AggrOpMap {
	public static final Logger logger = Logger.getLogger(AggrOpMap.class);
	Map<Integer, AggrOperator> opMap = new HashMap<Integer, AggrOperator>();
	private List<AggrOp> oplist = new ArrayList<AggrOp>();
	
	public AggrOpMap(List<Object> strAggrOpList){
		for (Object aggrOp:strAggrOpList){
			String strAggrOp = (String)aggrOp;
			String[] arr = strAggrOp.split(AggrOp.AGGR_OPERATOR_SEP);
			String op = arr[0];
			String arg = arr[1];
			List<IdxRange> irl = IdxRange.parseString(arg);
			if (AggrOp.AGGR_OP_SUM.equals(op)){
				oplist.add(new AggrOp(AggrOperator.sum, irl));
			}else{
				logger.error(String.format("op %s not supported.", op));
			}
		}
	}
	
	public void addAggrOp(AggrOp aop){
		oplist.add(aop);
	}
	
	public void constructMap(int idxMax){
		for (AggrOp aop : oplist){
			for (IdxRange ir: aop.getIdxRangeList()){
				int start = ir.getStart();
				int end = ir.getEnd();
				if (ir.getEnd()==-1){
					end = idxMax;
				}
				for (int i=start; i<=end; i++){
					opMap.put(i, aop.getAo());
				}
			}
		}
	}
	
	public AggrOperator getOp(int idx){
		if (opMap.containsKey(idx)){
			return opMap.get(idx);
		}else{
			return null;
		}
	}
}
