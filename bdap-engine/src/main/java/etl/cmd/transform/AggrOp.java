package etl.cmd.transform;

import java.io.Serializable;
import java.util.List;

import etl.engine.AggrOperator;
import etl.util.IdxRange;

public class AggrOp implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static final String AGGR_OP_SUM="sum";
	public static final String AGGR_OPERATOR_SEP="\\|";
	
	private AggrOperator ao;
	private List<IdxRange> idxRangeList;
	
	public AggrOp(AggrOperator ao, List<IdxRange> idxRangeList){
		this.setAo(ao);
		this.setIdxRangeList(idxRangeList);
	}

	public List<IdxRange> getIdxRangeList() {
		return idxRangeList;
	}

	public void setIdxRangeList(List<IdxRange> idxRangeList) {
		this.idxRangeList = idxRangeList;
	}

	public AggrOperator getAo() {
		return ao;
	}

	public void setAo(AggrOperator ao) {
		this.ao = ao;
	}
}
