package etl.cmd.transform;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.AggrOperator;
import etl.util.IdxRange;

public class AggrOps implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(AggrOps.class);
	
	private List<AggrOp> oplist = new ArrayList<AggrOp>();
	
	public AggrOps(){	
	}
	
	public AggrOps(String[] strAggrOpList){
		for (String strAggrOp:strAggrOpList){
			String[] arr = strAggrOp.split(AggrOp.AGGR_OPERATOR_SEP, -1);
			String op = arr[0];
			List<IdxRange> irl = null;
			if (arr.length>1){
				irl = IdxRange.parseString(arr[1]);
			}
			oplist.add(new AggrOp(AggrOperator.valueOf(op), irl));
		}
	}
	
	public void addAggrOp(AggrOp aop){
		oplist.add(aop);
	}
	
	public List<AggrOp> getOpList(){
		return oplist;
	}
}
